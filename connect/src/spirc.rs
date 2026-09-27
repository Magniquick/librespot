use crate::{
    LoadContextOptions, LoadRequestOptions, PlayContext,
    context_resolver::{ContextAction, ContextResolver, ResolveContext},
    core::{
        Error, Session, SpotifyUri,
        authentication::Credentials,
        dealer::protocol::TransferOptions,
        dealer::{
            manager::{BoxedStream, BoxedStreamResult, Reply, RequestReply},
            protocol::{Command, FallbackWrapper, Message, Request},
        },
        session::UserAttributes,
        spclient::TransferRequest,
    },
    model::{LoadRequest, PlayingTrack, SpircPlayStatus},
    playback::{
        mixer::Mixer,
        player::{Player, PlayerEvent, PlayerEventChannel},
    },
    protocol::{
        connect::{Cluster, ClusterUpdate, DeviceInfo, LogoutCommand, SetVolumeCommand},
        context::Context,
        explicit_content_pubsub::UserAttributesUpdate,
        playlist4_external::PlaylistModificationInfo,
        social_connect_v2::SessionUpdate,
        transfer_state::TransferState,
        user_attributes::UserAttributesMutation,
        useraccount::{AccountAttribute, account_attribute::Value},
    },
    state::{
        context::{ContextType, ResetContext},
        provider::IsProvider,
        {ConnectConfig, ConnectState},
    },
};
use futures_util::StreamExt;
use librespot_protocol::{context_page::ContextPage, playback::Playback};
use protobuf::MessageField;
use std::{
    collections::HashMap,
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant as TokioInstant, sleep, sleep_until},
};

#[derive(Debug, Error)]
enum SpircError {
    #[error("response payload empty")]
    NoData,
    #[error("{0} had no uri")]
    NoUri(&'static str),
    #[error("message pushed for another URI")]
    InvalidUri(String),
    #[error("failed to put connect state for new device")]
    FailedDealerSetup,
    #[error("unknown endpoint: {0:#?}")]
    UnknownEndpoint(serde_json::Value),
}

impl From<SpircError> for Error {
    fn from(err: SpircError) -> Self {
        use SpircError::*;
        match err {
            NoData | NoUri(_) => Error::unavailable(err),
            InvalidUri(_) | FailedDealerSetup => Error::aborted(err),
            UnknownEndpoint(_) => Error::unimplemented(err),
        }
    }
}

struct SpircTask {
    disconnected_playback: Arc<Mutex<Option<Arc<PlaybackSnapshot>>>>,
    player: Arc<Player>,
    mixer: Arc<dyn Mixer>,

    /// the state management object
    connect_state: ConnectState,
    connect_established: bool,

    play_request_id: Option<u64>,
    play_status: SpircPlayStatus,

    connection_id_update: BoxedStreamResult<String>,
    connect_state_update: BoxedStreamResult<ClusterUpdate>,
    connect_state_volume_update: BoxedStreamResult<SetVolumeCommand>,
    connect_state_logout_request: BoxedStreamResult<LogoutCommand>,
    playlist_update: BoxedStreamResult<PlaylistModificationInfo>,
    session_update: BoxedStreamResult<FallbackWrapper<SessionUpdate>>,
    connect_state_command: BoxedStream<RequestReply>,
    user_attributes_update: BoxedStreamResult<UserAttributesUpdate>,
    user_attributes_mutation: BoxedStreamResult<UserAttributesMutation>,

    commands: Option<mpsc::UnboundedReceiver<SpircCommand>>,
    player_events: Option<PlayerEventChannel>,

    context_resolver: ContextResolver,

    shutdown: bool,
    session: Session,

    /// is set when transferring, and used after resolving the contexts to finish the transfer
    pub transfer_state: Option<TransferState>,
    /// A track that ended (for example a transferred position past its end)
    /// while the transfer was still being set up, keyed by its play request.
    /// Handled once the transfer's context is resolved.
    pending_end_of_track: Option<u64>,
    /// The devices in the latest cluster, to name whoever sends a command.
    devices: HashMap<String, DeviceInfo>,
    /// The remote device or Web API app controlling playback; `None` when
    /// playback was started here.
    controller: Option<Controller>,
    /// What the last session_client_changed event reported.
    reported_client: Option<Controller>,
    /// A command sender the device list didn't name yet; reported once it does.
    unresolved_sender: Option<String>,
    /// The active device in the latest cluster.
    active_device_id: String,
    /// A transfer this device requested, answered once its target is active.
    pending_transfer_to: Option<PendingTransfer>,
    transfer_requests: u64,
    background_tx: mpsc::UnboundedSender<Background>,
    background_rx: mpsc::UnboundedReceiver<Background>,
    /// The user customization service answered for autoplay this connection;
    /// don't ask again on every dealer reconnect.
    autoplay_fetched: bool,
    attribute_requests: u64,
    /// Keys asked for but not yet applied, with how many change notices named
    /// each (for the flip fallback); a newer fetch covers them too.
    pending_attribute_keys: Vec<(String, u32)>,

    /// when set to true, it will update the volume after [VOLUME_UPDATE_DELAY],
    /// when no other future resolves, otherwise resets the delay
    update_volume: bool,

    /// when set to true, it will update the volume after [UPDATE_STATE_DELAY],
    /// when no other future resolves, otherwise resets the delay
    update_state: bool,

    spirc_id: usize,
}

static SPIRC_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
enum SpircCommand {
    Restore(Arc<PlaybackSnapshot>),
    Play,
    PlayPause,
    Pause,
    Prev,
    Next,
    ClearQueue,
    AddToQueue(String),
    VolumeUp,
    VolumeDown,
    Shutdown,
    Shuffle(bool),
    Repeat(bool),
    RepeatTrack(bool),
    Disconnect {
        pause: bool,
    },
    SetPosition(u32),
    SetVolume(u16),
    Activate,
    Transfer(Option<TransferRequest>),
    TransferTo {
        target: String,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Load(LoadRequest),
}

const CONTEXT_FETCH_THRESHOLD: usize = 2;

// delay to update volume after a certain amount of time, instead on each update request
const VOLUME_UPDATE_DELAY: Duration = Duration::from_millis(500);
// to reduce updates to remote, we group some request by waiting for a set amount of time
const UPDATE_STATE_DELAY: Duration = Duration::from_millis(200);
// how long a transfer target may take to become the active device, as the official clients wait
const BECOME_ACTIVE_TIMEOUT: Duration = Duration::from_secs(30);

/// A transfer this device requested, answered once its target is active.
struct PendingTransfer {
    /// Tells this request's outcome apart from an earlier one to the same target.
    id: u64,
    target: String,
    reply: oneshot::Sender<Result<(), Error>>,
    deadline: TokioInstant,
}

/// Results of requests the task runs off its loop, so HTTP never stalls it.
enum Background {
    Attributes {
        /// Only the newest fetch is applied: an older one may answer last.
        request: u64,
        result: Result<HashMap<String, AccountAttribute>, Error>,
    },
    TransferFailed {
        id: u64,
        error: Error,
    },
}

/// Where a transferred track resumes, as the official clients compute it: the
/// published position moved on at the published speed since it was recorded,
/// including from 0 (the track had just started). A paused or buffering source
/// publishes speed 0, and a missing speed counts as 0. Unlike the official
/// clients, a missing timestamp leaves the position as published rather than
/// extrapolating from the epoch. Out-of-range data is clamped rather than
/// failing the transfer; a position past the end is left to the player.
fn transfer_position(playback: &Playback, now_ms: i64) -> u32 {
    let reported = i64::from(playback.position_as_of_timestamp.unwrap_or_default());
    let timestamp = playback.timestamp.unwrap_or_default();
    let position = if playback.is_paused.unwrap_or_default() || timestamp <= 0 {
        reported
    } else {
        let speed = playback.playback_speed.unwrap_or_default().max(0.0);
        let elapsed = now_ms.saturating_sub(timestamp).max(0) as f64;
        // float-to-int casts saturate; the sum must too
        reported.saturating_add((elapsed * speed) as i64)
    };
    position.clamp(0, i64::from(u32::MAX)) as u32
}

/// An account attribute in the form the session caches it ("1"/"0" for booleans).
fn attribute_value(attribute: Option<&AccountAttribute>) -> Option<String> {
    match attribute.and_then(|attribute| attribute.value.as_ref())? {
        Value::BoolValue(value) => Some(if *value { "1" } else { "0" }.to_string()),
        Value::LongValue(value) => Some(value.to_string()),
        Value::StringValue(value) => Some(value.clone()),
        _ => None,
    }
}

/// The client reported in session_client_changed events.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Controller {
    client_id: String,
    name: String,
    brand: String,
    model: String,
}

/// The spotify connect handle
pub struct Spirc {
    commands: mpsc::UnboundedSender<SpircCommand>,
    disconnected_playback: Arc<Mutex<Option<Arc<PlaybackSnapshot>>>>,
}

/// In-memory playback retained when an active Connect session unexpectedly ends.
///
/// This includes the resolved contexts, exact shuffled order, previous and next
/// tracks, manual queue, repeat settings, and unresolved context pages. It can
/// only be restored by the same account. It contains no login credentials and
/// is not a persistent storage format.
pub struct PlaybackSnapshot {
    state: ConnectState,
    pending: std::collections::VecDeque<ResolveContext>,
    username: String,
    position_ms: u32,
    playing: bool,
}

impl PlaybackSnapshot {
    fn restore_into(
        &self,
        username: &str,
        state: &mut ConnectState,
        resolver: &mut ContextResolver,
    ) -> Result<(), Error> {
        if self.username != username {
            return Err(Error::failed_precondition(
                "playback belongs to another account",
            ));
        }
        state.restore_playback(&self.state);
        resolver.restore_pending(self.pending.clone());
        Ok(())
    }
}

impl std::fmt::Debug for PlaybackSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlaybackSnapshot")
            .field("position_ms", &self.position_ms)
            .field("playing", &self.playing)
            .finish_non_exhaustive()
    }
}

impl Spirc {
    /// Playback retained before an unexpected disconnect clears session state.
    /// Available after the event-loop future returned, and absent for an
    /// inactive device, stopped playback, or an intentional shutdown.
    pub fn disconnected_playback(&self) -> Option<Arc<PlaybackSnapshot>> {
        self.disconnected_playback
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Restore a disconnected session on this device, retaining queue order.
    /// New playback already active on this device takes precedence.
    pub fn restore_playback(&self, snapshot: Arc<PlaybackSnapshot>) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Restore(snapshot))?)
    }

    /// Initializes a new spotify connect device
    ///
    /// The returned tuple consists out of a handle to the [`Spirc`] that
    /// can control the local connect device when active. And a [`Future`]
    /// which represents the [`Spirc`] event loop that processes the whole
    /// connect device logic.
    pub async fn new(
        config: ConnectConfig,
        session: Session,
        credentials: Credentials,
        player: Arc<Player>,
        mixer: Arc<dyn Mixer>,
    ) -> Result<(Spirc, impl Future<Output = ()>), Error> {
        fn extract_connection_id(msg: Message) -> Result<String, Error> {
            let connection_id = msg
                .headers
                .get("Spotify-Connection-Id")
                .ok_or_else(|| SpircError::InvalidUri(msg.uri.clone()))?;
            Ok(connection_id.to_owned())
        }

        let spirc_id = SPIRC_COUNTER.fetch_add(1, Ordering::AcqRel);
        debug!("new Spirc[{spirc_id}]");

        let connect_state = ConnectState::new(config, &session);

        let connection_id_update = session
            .dealer()
            .listen_for("hm://pusher/v1/connections/", extract_connection_id)?;

        let connect_state_update = session
            .dealer()
            .listen_for("hm://connect-state/v1/cluster", Message::from_raw)?;

        let connect_state_volume_update = session
            .dealer()
            .listen_for("hm://connect-state/v1/connect/volume", Message::from_raw)?;

        let connect_state_logout_request = session
            .dealer()
            .listen_for("hm://connect-state/v1/connect/logout", Message::from_raw)?;

        let playlist_update = session
            .dealer()
            .listen_for("hm://playlist/v2/playlist/", Message::from_raw)?;

        let session_update = session
            .dealer()
            .listen_for("social-connect/v2/session_update", Message::try_from_json)?;

        let user_attributes_update = session
            .dealer()
            .listen_for("spotify:user:attributes:update", Message::from_raw)?;

        // can be trigger by toggling autoplay in a desktop client
        let user_attributes_mutation = session
            .dealer()
            .listen_for("spotify:user:attributes:mutated", Message::from_raw)?;

        let connect_state_command = session
            .dealer()
            .handle_for("hm://connect-state/v1/player/command")?;

        // pre-acquire client_token, preventing multiple request while running
        let _ = session.spclient().client_token().await?;

        // Connect *after* all message listeners are registered
        session.connect(credentials, true).await?;

        // pre-acquire access_token (we need to be authenticated to retrieve a token)
        let _ = session.login5().auth_token().await?;

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let player_events = player.get_player_event_channel();

        let disconnected_playback = Arc::new(Mutex::new(None));
        let (background_tx, background_rx) = mpsc::unbounded_channel();
        let mut task = SpircTask {
            disconnected_playback: Arc::clone(&disconnected_playback),
            player,
            mixer,

            connect_state,
            connect_established: false,

            play_request_id: None,
            play_status: SpircPlayStatus::Stopped,

            connection_id_update,
            connect_state_update,
            connect_state_volume_update,
            connect_state_logout_request,
            playlist_update,
            session_update,
            connect_state_command,
            user_attributes_update,
            user_attributes_mutation,
            commands: Some(cmd_rx),
            player_events: Some(player_events),

            context_resolver: ContextResolver::new(session.clone()),

            shutdown: false,
            session,

            transfer_state: None,
            pending_end_of_track: None,
            devices: HashMap::new(),
            controller: None,
            reported_client: None,
            unresolved_sender: None,
            active_device_id: String::new(),
            pending_transfer_to: None,
            transfer_requests: 0,
            background_tx,
            background_rx,
            autoplay_fetched: false,
            attribute_requests: 0,
            pending_attribute_keys: Vec::new(),
            update_volume: false,
            update_state: false,

            spirc_id,
        };

        let spirc = Spirc {
            commands: cmd_tx,
            disconnected_playback,
        };

        let initial_volume = task.connect_state.device_info().volume;
        task.connect_state.set_volume(0);

        match initial_volume.try_into() {
            Ok(volume) => {
                task.set_volume(volume);
                // we don't want to update the volume initially,
                // we just want to set the mixer to the correct volume
                task.update_volume = false;
            }
            Err(why) => error!("failed to update initial volume: {why}"),
        };

        Ok((spirc, task.run()))
    }

    /// Safely shutdowns the spirc.
    ///
    /// This pauses the playback, disconnects the connect device and
    /// bring the future initially returned to an end.
    pub fn shutdown(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Shutdown)?)
    }

    /// Resumes the playback
    ///
    /// Does nothing if we are not the active device, or it isn't paused.
    pub fn play(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Play)?)
    }

    /// Resumes or pauses the playback
    ///
    /// Does nothing if we are not the active device.
    pub fn play_pause(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::PlayPause)?)
    }

    /// Pauses the playback
    ///
    /// Does nothing if we are not the active device, or if it isn't playing.
    pub fn pause(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Pause)?)
    }

    /// Seeks to the beginning or skips to the previous track.
    ///
    /// Seeks to the beginning when the current track position
    /// is greater than 3 seconds.
    ///
    /// Does nothing if we are not the active device.
    pub fn prev(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Prev)?)
    }

    /// Skips to the next track.
    ///
    /// Does nothing if we are not the active device.
    pub fn next(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Next)?)
    }

    /// Removes every queued track, keeping the playing context's own tracks.
    ///
    /// Does nothing if we are not the active device.
    pub fn clear_queue(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::ClearQueue)?)
    }

    /// Adds a track or episode to the queue, after the tracks already
    /// queued there and before the playing context's own.
    ///
    /// Does nothing if we are not the active device.
    pub fn add_to_queue(&self, uri: String) -> Result<(), Error> {
        if !uri.starts_with("spotify:track:") && !uri.starts_with("spotify:episode:") {
            return Err(Error::invalid_argument("uri"));
        }
        Ok(self.commands.send(SpircCommand::AddToQueue(uri))?)
    }

    /// Increases the volume by configured steps of [ConnectConfig].
    ///
    /// Does nothing if we are not the active device.
    pub fn volume_up(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::VolumeUp)?)
    }

    /// Decreases the volume by configured steps of [ConnectConfig].
    ///
    /// Does nothing if we are not the active device.
    pub fn volume_down(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::VolumeDown)?)
    }

    /// Shuffles the playback according to the value.
    ///
    /// If true shuffles/reshuffles the playback. Otherwise, does
    /// nothing (if not shuffled) or unshuffles the playback while
    /// resuming at the position of the current track.
    ///
    /// Does nothing if we are not the active device.
    pub fn shuffle(&self, shuffle: bool) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Shuffle(shuffle))?)
    }

    /// Repeats the playback context according to the value.
    ///
    /// Does nothing if we are not the active device.
    pub fn repeat(&self, repeat: bool) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Repeat(repeat))?)
    }

    /// Repeats the current track if true.
    ///
    /// Does nothing if we are not the active device.
    ///
    /// Skipping to the next track disables the repeating.
    pub fn repeat_track(&self, repeat: bool) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::RepeatTrack(repeat))?)
    }

    /// Update the volume to the given value.
    ///
    /// Does nothing if we are not the active device.
    pub fn set_volume(&self, volume: u16) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::SetVolume(volume))?)
    }

    /// Updates the position to the given value.
    ///
    /// Does nothing if we are not the active device.
    ///
    /// If value is greater than the track duration,
    /// the update is ignored.
    pub fn set_position_ms(&self, position_ms: u32) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::SetPosition(position_ms))?)
    }

    /// Load a new context and replace the current.
    ///
    /// Does nothing if we are not the active device.
    ///
    /// Does not overwrite the queue.
    pub fn load(&self, command: LoadRequest) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Load(command))?)
    }

    /// Disconnects the current device and pauses the playback according the value.
    ///
    /// Does nothing if we are not the active device.
    pub fn disconnect(&self, pause: bool) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Disconnect { pause })?)
    }

    /// Acquires the control as active connect device.
    ///
    /// Does not [Spirc::transfer] the playback. Does nothing if we are not the active device.
    pub fn activate(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Activate)?)
    }

    /// Acquires the control as active connect device over the transfer flow.
    ///
    /// Does nothing if we are not the active device.
    pub fn transfer(&self, transfer_request: Option<TransferRequest>) -> Result<(), Error> {
        Ok(self
            .commands
            .send(SpircCommand::Transfer(transfer_request))?)
    }

    /// Hands playback to another device through the Connect transfer flow, the
    /// way the official clients do: the current state is published first, so the
    /// target resumes exactly where playback is, then the transfer is requested.
    /// Works from this device, or between two other devices.
    ///
    /// The receiver resolves when the target is the active device, or with an
    /// error when the request fails, a newer transfer replaces it, or the target
    /// doesn't become active within 30 seconds.
    pub fn transfer_to(
        &self,
        device_id: impl Into<String>,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        let (reply, receiver) = oneshot::channel();
        self.commands.send(SpircCommand::TransferTo {
            target: device_id.into(),
            reply,
        })?;
        Ok(receiver)
    }
}

impl SpircTask {
    async fn run(mut self) {
        // simplify unwrapping of received item or parsed result
        macro_rules! unwrap {
            ( $next:expr, |$some:ident| $use_some:expr ) => {
                match $next {
                    Some($some) => $use_some,
                    None => {
                        error!("{} selected, but none received", stringify!($next));
                        break;
                    }
                }
            };
            ( $next:expr, match |$ok:ident| $use_ok:expr ) => {
                unwrap!($next, |$ok| match $ok {
                    Ok($ok) => $use_ok,
                    Err(why) => error!("could not parse {}: {}", stringify!($ok), why),
                })
            };
        }

        if let Err(why) = self.session.dealer().start().await {
            error!("starting dealer failed: {why}");
            return;
        }

        while !self.session.is_invalid() && !self.shutdown {
            let commands = self.commands.as_mut();
            let player_events = self.player_events.as_mut();

            // when state and volume update have a higher priority than context resolving
            // because of that the context resolving has to wait, so that the other tasks can finish
            let allow_context_resolving = !self.update_state && !self.update_volume;
            let transfer_deadline = self
                .pending_transfer_to
                .as_ref()
                .map(|pending| pending.deadline);

            tokio::select! {
                // startup of the dealer requires a connection_id, which is retrieved at the very beginning
                connection_id_update = self.connection_id_update.next() => unwrap! {
                    connection_id_update,
                    match |connection_id| if let Err(why) = self.handle_connection_id_update(connection_id).await {
                        error!("failed handling connection id update: {why}");
                        break;
                    }
                },
                // main dealer update of any remote device updates
                cluster_update = self.connect_state_update.next() => unwrap! {
                    cluster_update,
                    match |cluster_update| if let Err(e) = self.handle_cluster_update(cluster_update).await {
                        error!("could not dispatch connect state update: {e}");
                    }
                },
                // main dealer request handling (dealer expects an answer)
                request = self.connect_state_command.next() => unwrap! {
                    request,
                    |request| if let Err(e) = self.handle_connect_state_request(request).await {
                        error!("couldn't handle connect state command: {e}");
                    }
                },
                // volume request handling is send separately (it's more like a fire forget)
                volume_update = self.connect_state_volume_update.next() => unwrap! {
                    volume_update,
                    match |volume_update| match volume_update.volume.try_into() {
                        Ok(volume) => self.set_volume(volume),
                        Err(why) => error!("can't update volume, failed to parse i32 to u16: {why}")
                    }
                },
                logout_request = self.connect_state_logout_request.next() => unwrap! {
                    logout_request,
                    |logout_request| {
                        error!("received logout request, currently not supported: {logout_request:#?}");
                        // todo: call logout handling
                    }
                },
                playlist_update = self.playlist_update.next() => unwrap! {
                    playlist_update,
                    match |playlist_update| if let Err(why) = self.handle_playlist_modification(playlist_update) {
                        error!("failed to handle playlist modification: {why}")
                    }
                },
                user_attributes_update = self.user_attributes_update.next() => unwrap! {
                    user_attributes_update,
                    match |attributes| self.handle_user_attributes_update(attributes)
                },
                user_attributes_mutation = self.user_attributes_mutation.next() => unwrap! {
                    user_attributes_mutation,
                    match |attributes| self.handle_user_attributes_mutation(attributes)
                },
                session_update = self.session_update.next() => unwrap! {
                    session_update,
                    match |session_update| self.handle_session_update(session_update)
                },
                cmd = async { commands?.recv().await }, if commands.is_some() && self.connect_established => if let Some(cmd) = cmd {
                    if let Err(e) = self.handle_command(cmd).await {
                        debug!("could not dispatch command: {e}");
                    }
                },
                event = async { player_events?.recv().await }, if player_events.is_some() => if let Some(event) = event {
                    if let Err(e) = self.handle_player_event(event) {
                        error!("could not dispatch player event: {e}");
                    }
                },
                background = self.background_rx.recv() => if let Some(background) = background {
                    self.handle_background(background);
                },
                _ = async { sleep_until(transfer_deadline.unwrap_or_else(TokioInstant::now)).await }, if transfer_deadline.is_some() => {
                    if let Some(PendingTransfer { target, reply, .. }) = self.pending_transfer_to.take() {
                        warn!("transfer target <{target}> did not become active");
                        let _ = reply.send(Err(Error::deadline_exceeded(format!(
                            "{target} did not become the active device"
                        ))));
                    }
                },
                _ = async { sleep(UPDATE_STATE_DELAY).await }, if self.update_state => {
                    self.update_state = false;

                    if let Err(why) = self.notify().await {
                        error!("state update: {why}")
                    }
                },
                _ = async { sleep(VOLUME_UPDATE_DELAY).await }, if self.update_volume => {
                    self.update_volume = false;

                    info!("delayed volume update for all devices: volume is now {}", self.connect_state.device_info().volume);
                    if let Err(why) = self.connect_state.notify_volume_changed(&self.session).await {
                        error!("error updating connect state for volume update: {why}")
                    }

                    // for some reason the web-player does need two separate updates, so that the
                    // position of the current track is retained, other clients also send a state
                    // update before they send the volume update
                    if let Err(why) = self.notify().await {
                        error!("error updating connect state for volume update: {why}")
                    }
                },
                // context resolver handling, the idea/reason behind it the following:
                //
                // when we request a context that has multiple pages (for example an artist)
                // resolving all pages at once can take around ~1-30sec, when we resolve
                // everything at once that would block our main loop for that time
                //
                // to circumvent this behavior, we request each context separately here and
                // finish after we received our last item of a type
                next_context = async {
                    self.context_resolver.get_next_context(|| {
                        // Sending local file URIs to this endpoint results in a Bad Request status.
                        // It's likely appropriate to filter them out anyway; Spotify's backend
                        // has no knowledge about these tracks and so can't do anything with them.
                        self.connect_state.recent_track_uris()
                            .into_iter()
                            .filter(|t| !t.starts_with("spotify:local"))
                            .collect::<Vec<_>>()
                    }).await
                }, if allow_context_resolving && self.context_resolver.has_next() => {
                    let update_state = self.handle_next_context(next_context);
                    if update_state {
                        if let Err(why) = self.notify().await {
                            error!("update after context resolving failed: {why}")
                        }
                    }
                },
                else => break
            }
        }

        if !self.shutdown && self.connect_state.is_active() {
            if !matches!(self.play_status, SpircPlayStatus::Stopped)
                && self.transfer_state.is_none()
                && self.connect_state.current_track(MessageField::is_some)
            {
                let snapshot = PlaybackSnapshot {
                    state: self.connect_state.clone(),
                    pending: self.context_resolver.pending(),
                    username: self.session.username(),
                    position_ms: self.position(),
                    playing: self.connect_state.is_playing(),
                };
                *self
                    .disconnected_playback
                    .lock()
                    .unwrap_or_else(|p| p.into_inner()) = Some(Arc::new(snapshot));
            }
            warn!("unexpected shutdown");
            if let Err(why) = self.handle_disconnect().await {
                error!("error during disconnecting: {why}")
            }
        }

        // this should clear the active session id, leaving an empty state
        if let Err(why) = self.session.spclient().delete_connect_state_request().await {
            error!("error during connect state deletion: {why}")
        };

        self.session.dealer().close().await;
    }

    /// Answers the pending transfer once the cluster shows its target active.
    fn resolve_pending_transfer(&mut self) {
        let arrived = matches!(
            &self.pending_transfer_to,
            Some(pending) if pending.target == self.active_device_id
        );
        if arrived {
            if let Some(PendingTransfer { target, reply, .. }) = self.pending_transfer_to.take() {
                debug!("transfer to <{target}> completed");
                let _ = reply.send(Ok(()));
            }
        }
    }

    async fn handle_transfer_to(
        &mut self,
        target: String,
        reply: oneshot::Sender<Result<(), Error>>,
    ) {
        // A newer transfer replaces an older one, whatever becomes of it.
        if let Some(PendingTransfer {
            target: replaced,
            reply: previous,
            ..
        }) = self.pending_transfer_to.take()
        {
            // Cancelled, not Aborted: Aborted also reports dropped connections.
            let _ = previous.send(Err(Error::cancelled(format!(
                "the transfer to {replaced} was replaced by one to {target}"
            ))));
        }
        let from = if self.connect_state.is_active() {
            self.session.device_id().to_string()
        } else {
            self.active_device_id.clone()
        };
        if from == target {
            let _ = reply.send(Ok(()));
            return;
        }
        if from.is_empty() {
            let _ = reply.send(Err(Error::failed_precondition(
                "no active device to transfer from",
            )));
            return;
        }
        // The target resumes from the published state. Changes are published as
        // they happen, and Spotify moves the position on from the published
        // timestamp; only a change still waiting to be published needs sending
        // before the transfer is requested.
        if self.connect_state.is_active() && self.update_state {
            self.update_state = false;
            if let Err(why) = self.notify().await {
                warn!("couldn't publish the state before transferring: {why}");
            }
        }
        // Answered by the cluster update that shows the target active, which may
        // arrive before the request's own response.
        self.transfer_requests += 1;
        let id = self.transfer_requests;
        self.pending_transfer_to = Some(PendingTransfer {
            id,
            target: target.clone(),
            reply,
            deadline: TokioInstant::now() + BECOME_ACTIVE_TIMEOUT,
        });
        let request = TransferRequest {
            transfer_options: TransferOptions {
                restore_paused: Some("restore".into()),
                restore_position: Some("extrapolate".into()),
                restore_track: Some("only_current".into()),
                retain_session: None,
            },
        };
        let session = self.session.clone();
        let background = self.background_tx.clone();
        self.session.spawn(async move {
            if let Err(error) = session
                .spclient()
                .transfer(&from, &target, Some(&request))
                .await
            {
                let _ = background.send(Background::TransferFailed { id, error });
            }
        });
    }

    /// Continues after a track that ended while the transfer was being set up,
    /// once there is something to continue with.
    fn resume_pending_end_of_track(&mut self) {
        let Some(ended) = self.pending_end_of_track else {
            return;
        };
        if Some(ended) != self.play_request_id {
            // A newer playback replaced the track that ended.
            self.pending_end_of_track = None;
            return;
        }
        let resolving = self.context_resolver.has_next();
        // The transfer finishes when its context is resolved.
        if self.transfer_state.is_some() && resolving {
            return;
        }
        // At the end of the context, autoplay may still be on its way.
        if resolving
            && !self.connect_state.repeat_track()
            && !self.connect_state.has_next_tracks(None)
        {
            return;
        }
        self.pending_end_of_track = None;
        if let Err(why) = self.handle_end_of_track() {
            error!("continuing after a track that ended during the transfer failed: {why}");
        }
    }

    fn handle_end_of_track(&mut self) -> Result<(), Error> {
        let next_track = self
            .connect_state
            .repeat_track()
            .then(|| self.connect_state.current_track(|t| t.uri.clone()));

        self.handle_next(next_track)
    }

    fn handle_next_context(&mut self, next_context: Result<Context, Error>) -> bool {
        let next_context = match next_context {
            Err(why) => {
                self.context_resolver.mark_next_unavailable();
                self.context_resolver.remove_used_and_invalid();
                error!("{why}");
                if !self.context_resolver.has_next() {
                    // The transfer's context can't be resolved: it will not finish.
                    self.transfer_state = None;
                }
                self.resume_pending_end_of_track();
                return false;
            }
            Ok(ctx) => ctx,
        };

        debug!("handling next context {:?}", next_context.uri);

        match self
            .context_resolver
            .apply_next_context(&mut self.connect_state, next_context)
        {
            Ok(remaining) => {
                if let Some(remaining) = remaining {
                    self.context_resolver.add_list(remaining)
                }
            }
            Err(why) => {
                error!("{why}")
            }
        }

        let update_state = if self
            .context_resolver
            .try_finish(&mut self.connect_state, &mut self.transfer_state)
        {
            self.add_autoplay_resolving_when_required();
            true
        } else {
            false
        };

        self.context_resolver.remove_used_and_invalid();
        self.resume_pending_end_of_track();
        update_state
    }

    // todo: is the time_delta still necessary?
    fn now_ms(&self) -> i64 {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|err| err.duration());

        dur.as_millis() as i64 + 1000 * self.session.time_delta()
    }

    async fn handle_command(&mut self, cmd: SpircCommand) -> Result<(), Error> {
        trace!("Received SpircCommand::{cmd:?}");
        match cmd {
            SpircCommand::Shutdown => {
                trace!("Received SpircCommand::Shutdown");
                self.handle_pause();
                self.handle_disconnect().await?;
                self.shutdown = true;
                if let Some(rx) = self.commands.as_mut() {
                    rx.close()
                }
            }
            SpircCommand::TransferTo { target, reply } => {
                self.handle_transfer_to(target, reply).await;
                return Ok(());
            }
            SpircCommand::Transfer(request) if !self.connect_state.is_active() => {
                // Spotify answers the request only after this task has handled the
                // transfer it sends back over the dealer, so the request can't be
                // awaited here. Pull from the active device, as the official
                // clients do.
                let to = self.session.device_id().to_string();
                let from = if self.active_device_id.is_empty() {
                    to.clone()
                } else {
                    self.active_device_id.clone()
                };
                let session = self.session.clone();
                self.session.spawn(async move {
                    if let Err(why) = session
                        .spclient()
                        .transfer(&from, &to, request.as_ref())
                        .await
                    {
                        warn!("transfer to this device failed: {why}");
                    }
                });
                return Ok(());
            }
            SpircCommand::Activate if !self.connect_state.is_active() => {
                trace!("Received SpircCommand::{cmd:?}");
                self.controller = None;
                self.unresolved_sender = None;
                self.handle_activate();
                return self.notify().await;
            }
            SpircCommand::Transfer(..) | SpircCommand::Activate => {
                warn!("SpircCommand::{cmd:?} will be ignored while already active")
            }
            SpircCommand::Restore(snapshot) => {
                if self.connect_state.is_active()
                    && !matches!(self.play_status, SpircPlayStatus::Stopped)
                {
                    return Ok(());
                }
                snapshot.restore_into(
                    &self.session.username(),
                    &mut self.connect_state,
                    &mut self.context_resolver,
                )?;
                self.handle_activate();
                self.load_track(snapshot.playing, snapshot.position_ms)?;
            }
            _ if !self.connect_state.is_active() => {
                warn!("SpircCommand::{cmd:?} will be ignored while Not Active")
            }
            SpircCommand::Disconnect { pause } => {
                if pause {
                    self.handle_pause()
                }
                return self.handle_disconnect().await;
            }
            SpircCommand::Play => self.handle_play(),
            SpircCommand::PlayPause => self.handle_play_pause(),
            SpircCommand::Pause => self.handle_pause(),
            SpircCommand::Prev => self.handle_prev()?,
            SpircCommand::Next => self.handle_next(None)?,
            SpircCommand::ClearQueue => self.handle_clear_queue(),
            SpircCommand::AddToQueue(uri) => self.handle_add_to_queue(uri),
            SpircCommand::VolumeUp => self.handle_volume_up(),
            SpircCommand::VolumeDown => self.handle_volume_down(),
            SpircCommand::Shuffle(shuffle) => self.handle_shuffle(shuffle)?,
            SpircCommand::Repeat(repeat) => self.handle_repeat_context(repeat)?,
            SpircCommand::RepeatTrack(repeat) => self.handle_repeat_track(repeat),
            SpircCommand::SetPosition(position) => self.handle_seek(position),
            SpircCommand::SetVolume(volume) => self.set_volume(volume),
            SpircCommand::Load(command) => {
                self.controller = None;
                self.unresolved_sender = None;
                self.handle_load(command, None, None).await?;
                self.report_client(false);
            }
        };

        self.notify().await
    }

    fn handle_player_event(&mut self, event: PlayerEvent) -> Result<(), Error> {
        if let PlayerEvent::TrackChanged { audio_item } = event {
            self.connect_state.update_duration(audio_item.duration_ms);
            self.update_state = true;
            return Ok(());
        }

        // update play_request_id
        if let PlayerEvent::PlayRequestIdChanged { play_request_id } = event {
            self.play_request_id = Some(play_request_id);
            return Ok(());
        }

        let is_current_track = matches! {
            (event.get_play_request_id(), self.play_request_id),
            (Some(event_id), Some(current_id)) if event_id == current_id
        };

        // we only process events if the play_request_id matches. If it doesn't, it is
        // an event that belongs to a previous track and only arrives now due to a race
        // condition. In this case we have updated the state already and don't want to
        // mess with it.
        if !is_current_track {
            return Ok(());
        }

        match event {
            PlayerEvent::EndOfTrack { .. } => {
                // Moving on needs the context the transfer is still resolving;
                // without it there is no next track and playback would stop.
                if self.transfer_state.is_some() && self.context_resolver.has_next() {
                    debug!("track ended while the transfer is being set up, continuing once it is");
                    self.pending_end_of_track = self.play_request_id;
                    return Ok(());
                }
                self.handle_end_of_track()?
            }
            PlayerEvent::Loading { .. } => match self.play_status {
                SpircPlayStatus::LoadingPlay { position_ms } => {
                    self.connect_state
                        .update_position(position_ms, self.now_ms());
                    trace!("==> LoadingPlay");
                }
                SpircPlayStatus::LoadingPause { position_ms } => {
                    self.connect_state
                        .update_position(position_ms, self.now_ms());
                    trace!("==> LoadingPause");
                }
                _ => {
                    self.connect_state.update_position(0, self.now_ms());
                    trace!("==> Loading");
                }
            },
            PlayerEvent::Seeked { position_ms, .. } => {
                trace!("==> Seeked");
                self.connect_state
                    .update_position(position_ms, self.now_ms())
            }
            PlayerEvent::Playing { position_ms, .. }
            | PlayerEvent::PositionCorrection { position_ms, .. } => {
                trace!("==> Playing");
                let new_nominal_start_time = self.now_ms() - position_ms as i64;
                match self.play_status {
                    SpircPlayStatus::Playing {
                        ref mut nominal_start_time,
                        ..
                    } => {
                        if (*nominal_start_time - new_nominal_start_time).abs() > 100 {
                            *nominal_start_time = new_nominal_start_time;
                            self.connect_state
                                .update_position(position_ms, self.now_ms());
                        } else {
                            return Ok(());
                        }
                    }
                    SpircPlayStatus::LoadingPlay { .. } | SpircPlayStatus::LoadingPause { .. } => {
                        self.connect_state
                            .update_position(position_ms, self.now_ms());
                        self.play_status = SpircPlayStatus::Playing {
                            nominal_start_time: new_nominal_start_time,
                            preloading_of_next_track_triggered: false,
                        };
                    }
                    _ => return Ok(()),
                }
            }
            PlayerEvent::Paused {
                position_ms: new_position_ms,
                ..
            } => {
                trace!("==> Paused");
                match self.play_status {
                    SpircPlayStatus::Paused { .. } | SpircPlayStatus::Playing { .. } => {
                        self.connect_state
                            .update_position(new_position_ms, self.now_ms());
                        self.play_status = SpircPlayStatus::Paused {
                            position_ms: new_position_ms,
                            preloading_of_next_track_triggered: false,
                        };
                    }
                    SpircPlayStatus::LoadingPlay { .. } | SpircPlayStatus::LoadingPause { .. } => {
                        self.connect_state
                            .update_position(new_position_ms, self.now_ms());
                        self.play_status = SpircPlayStatus::Paused {
                            position_ms: new_position_ms,
                            preloading_of_next_track_triggered: false,
                        };
                    }
                    _ => return Ok(()),
                }
            }
            PlayerEvent::Stopped { .. } => {
                trace!("==> Stopped");
                match self.play_status {
                    SpircPlayStatus::Stopped => return Ok(()),
                    _ => self.play_status = SpircPlayStatus::Stopped,
                }
            }
            PlayerEvent::TimeToPreloadNextTrack { .. } => {
                self.handle_preload_next_track();
                return Ok(());
            }
            PlayerEvent::Unavailable { track_id, .. } => {
                self.handle_unavailable(&track_id)?;
                if self.connect_state.current_track(|t| &t.uri) == &track_id.to_uri()? {
                    self.handle_next(None)?
                }
            }
            _ => return Ok(()),
        }

        self.update_state = true;
        Ok(())
    }

    async fn handle_connection_id_update(&mut self, connection_id: String) -> Result<(), Error> {
        trace!("Received connection ID update: {connection_id:?}");
        self.session.set_connection_id(&connection_id);

        let mut cluster = match self
            .connect_state
            .notify_new_device_appeared(&self.session)
            .await
        {
            Ok(res) => Cluster::parse_from_bytes(&res).ok(),
            Err(why) => {
                error!("{why:?}");
                None
            }
        }
        .ok_or(SpircError::FailedDealerSetup)?;
        self.devices = std::mem::take(&mut cluster.device);
        self.active_device_id = cluster.active_device_id.clone();
        self.resolve_pending_transfer();

        debug!(
            "successfully put connect state for {} with connection-id {connection_id}",
            self.session.device_id()
        );

        self.connect_established = true;

        if self.session.config().autoplay.is_none()
            && self.session.get_user_attribute("autoplay").is_none()
            && !self.autoplay_fetched
        {
            debug!("autoplay missing from the product info, asking the user customization service");
            self.request_account_attributes(vec!["autoplay".to_string()], false);
        }

        let same_session = cluster.player_state.session_id == self.session.session_id()
            || cluster.player_state.session_id.is_empty();
        if !cluster.active_device_id.is_empty() || !same_session {
            info!(
                "active device is <{}> with session <{}>",
                cluster.active_device_id, cluster.player_state.session_id
            );
            return Ok(());
        } else if cluster.transfer_data.is_empty() {
            debug!("got empty transfer state, do nothing");
            return Ok(());
        } else {
            info!(
                "trying to take over control automatically, session_id: {}",
                cluster.player_state.session_id
            )
        }

        use protobuf::Message;

        match TransferState::parse_from_bytes(&cluster.transfer_data) {
            Ok(transfer_state) => self.handle_transfer(transfer_state)?,
            Err(why) => error!("failed to take over control: {why}"),
        }

        Ok(())
    }

    fn handle_user_attributes_update(&mut self, update: UserAttributesUpdate) {
        trace!("Received attributes update: {update:#?}");
        let attributes: UserAttributes = update
            .pairs
            .iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();
        self.session.set_user_attributes(attributes)
    }

    fn handle_user_attributes_mutation(&mut self, mutation: UserAttributesMutation) {
        let keys = mutation
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        self.request_account_attributes(keys, true);
    }

    /// Asks the user customization service for the named account attributes.
    /// A mutation only names the changed field, and the AP's product info omits
    /// some attributes on some platforms (autoplay on Windows), so flipping a
    /// cached value either had nothing to flip or could invert the state after a
    /// duplicated or missed mutation. The request runs off the loop.
    fn request_account_attributes(&mut self, keys: Vec<String>, notices: bool) {
        let override_autoplay = self.session.config().autoplay.is_some();
        let keys: Vec<String> = keys
            .into_iter()
            .filter(|key| !(override_autoplay && key == "autoplay"))
            .collect();
        if keys.is_empty() {
            trace!("no account attribute to refresh (empty notice, or autoplay overridden)");
            return;
        }
        for key in keys {
            let notice = u32::from(notices);
            match self
                .pending_attribute_keys
                .iter_mut()
                .find(|(pending, _)| *pending == key)
            {
                Some((_, count)) => *count += notice,
                None => self.pending_attribute_keys.push((key, notice)),
            }
        }
        let keys: Vec<String> = self
            .pending_attribute_keys
            .iter()
            .map(|(key, _)| key.clone())
            .collect();
        // Keys named by an odd number of notices are expected to have changed.
        let old: Vec<Option<String>> = self
            .pending_attribute_keys
            .iter()
            .map(|(key, notices)| {
                if notices % 2 == 1 {
                    self.session.get_user_attribute(key)
                } else {
                    None
                }
            })
            .collect();
        self.attribute_requests += 1;
        let request = self.attribute_requests;
        let session = self.session.clone();
        let background = self.background_tx.clone();
        self.session.spawn(async move {
            let mut result = session.spclient().get_account_attributes().await;
            // The notice can arrive before the service reflects the change:
            // a value still equal to the old one earns one more look.
            let unchanged = match &result {
                Ok(attributes) => keys.iter().zip(&old).any(|(key, old)| {
                    old.is_some() && attribute_value(attributes.get(key)) == *old
                }),
                Err(_) => false,
            };
            if unchanged {
                sleep(Duration::from_secs(1)).await;
                result = session.spclient().get_account_attributes().await;
            }
            let _ = background.send(Background::Attributes { request, result });
        });
    }

    fn handle_background(&mut self, background: Background) {
        match background {
            Background::Attributes { request, result } => {
                if request == self.attribute_requests {
                    let keys = std::mem::take(&mut self.pending_attribute_keys);
                    self.apply_account_attributes(keys, result);
                } else {
                    trace!("dropping a superseded account attribute fetch");
                }
            }
            Background::TransferFailed { id, error } => {
                let failed = matches!(&self.pending_transfer_to, Some(pending) if pending.id == id);
                if failed {
                    if let Some(PendingTransfer { reply, .. }) = self.pending_transfer_to.take() {
                        let _ = reply.send(Err(error));
                    }
                }
            }
        }
    }

    fn apply_account_attributes(
        &mut self,
        keys: Vec<(String, u32)>,
        result: Result<HashMap<String, AccountAttribute>, Error>,
    ) {
        let fetched = match result {
            Ok(attributes) => {
                self.autoplay_fetched |= keys.iter().any(|(key, _)| key == "autoplay");
                Some(attributes)
            }
            Err(why) => {
                warn!("couldn't fetch account attributes, toggling the cached values: {why}");
                None
            }
        };

        for (key, notices) in &keys {
            let old_value = self.session.get_user_attribute(key);
            // Each notice flipped the value once: an even number leaves it.
            let flipped = || {
                old_value.as_deref().map(|old| match (old, notices % 2) {
                    ("0", 1) => "1".to_string(),
                    ("1", 1) => "0".to_string(),
                    (other, _) => other.to_string(),
                })
            };
            // A key the service doesn't report falls back to flipping, as before.
            let new_value = match &fetched {
                Some(attributes) => attribute_value(attributes.get(key)).or_else(flipped),
                None => flipped(),
            };
            let Some(new_value) = new_value else {
                trace!("no value is known for attribute {key}");
                continue;
            };
            if old_value.as_deref() == Some(new_value.as_str()) {
                continue;
            }
            self.session.set_user_attribute(key, &new_value);
            trace!("attribute {key} was {old_value:?} is now {new_value}");

            match key.as_str() {
                "filter-explicit-content" => self
                    .player
                    .emit_filter_explicit_content_changed_event(new_value == "1"),
                "autoplay" => {
                    self.player.emit_auto_play_changed_event(new_value == "1");
                    self.add_autoplay_resolving_when_required()
                }
                _ => {}
            }
        }
    }

    async fn handle_cluster_update(
        &mut self,
        mut cluster_update: ClusterUpdate,
    ) -> Result<(), Error> {
        let reason = cluster_update.update_reason.enum_value();

        let device_ids = cluster_update.devices_that_changed.join(", ");
        debug!(
            "cluster update: {reason:?} from {device_ids}, active device: {}",
            cluster_update.cluster.active_device_id
        );

        if let Some(mut cluster) = cluster_update.cluster.take() {
            self.devices = std::mem::take(&mut cluster.device);
            self.active_device_id = cluster.active_device_id.clone();
            if let Some(sender) = self.unresolved_sender.clone() {
                self.set_controller_from(&sender);
                if self.unresolved_sender.is_none() && self.connect_state.is_active() {
                    self.report_client(false);
                }
            }
            self.resolve_pending_transfer();
            let became_inactive = self.connect_state.is_active()
                && cluster.active_device_id != self.session.device_id();
            if became_inactive {
                info!("device became inactive");
                self.handle_disconnect().await?;
                self.handle_stop();
            } else if self.connect_state.is_active() {
                // fixme: workaround fix, because of missing information why it behaves like it does
                //  background: when another device sends a connect-state update, some player's position de-syncs
                //  tried: providing session_id, playback_id, track-metadata "track_player"
                self.update_state = true;
            }
        } else if self.connect_state.is_active() {
            self.connect_state.became_inactive(&self.session).await?;
        }

        Ok(())
    }

    async fn handle_connect_state_request(
        &mut self,
        (request, sender): RequestReply,
    ) -> Result<(), Error> {
        self.connect_state.set_last_command(request.clone());
        let previous_controller = (self.controller.clone(), self.unresolved_sender.clone());
        self.set_controller_from(&request.sent_by_device_id);

        debug!(
            "handling: '{}' from {}",
            request.command, request.sent_by_device_id
        );

        let response = match self.handle_request(request).await {
            Ok(_) => Reply::Success,
            Err(why) => {
                error!("failed to handle request: {why}");
                Reply::Failure
            }
        };
        // A different device took control of the playback we are playing.
        if matches!(response, Reply::Success) {
            if self.connect_state.is_active() {
                self.report_client(false);
            }
        } else {
            // A failed command took no control.
            (self.controller, self.unresolved_sender) = previous_controller;
        }

        sender.send(response).map_err(Into::into)
    }

    async fn handle_request(&mut self, request: Request) -> Result<(), Error> {
        use Command::*;

        match request.command {
            // errors and unknown commands
            Transfer(transfer) if transfer.data.is_none() => {
                warn!("transfer endpoint didn't contain any data to transfer");
                Err(SpircError::NoData)?
            }
            Unknown(unknown) => Err(SpircError::UnknownEndpoint(unknown))?,
            // implicit update of the connect_state
            UpdateContext(update_context) => {
                if matches!(update_context.context.uri, Some(ref uri) if uri != self.connect_state.context_uri())
                {
                    debug!(
                        "ignoring context update for <{:?}>, because it isn't the current context <{}>",
                        update_context.context.uri,
                        self.connect_state.context_uri()
                    )
                } else {
                    self.context_resolver.add(ResolveContext::from_context(
                        update_context.context,
                        ContextType::Default,
                        ContextAction::Replace,
                    ))
                }
                return Ok(());
            }
            // modification and update of the connect_state
            Transfer(transfer) => {
                self.handle_transfer(transfer.data.expect("by condition checked"))?;
                return self.notify().await;
            }
            Play(mut play) => {
                if !self.connect_state.is_active() {
                    self.handle_activate()
                }

                let context = match play.context.uri {
                    Some(s) => PlayContext::Uri(s),
                    None if !play.context.pages.is_empty() => PlayContext::Tracks(
                        play.context
                            .pages
                            .iter()
                            .cloned()
                            .flat_map(|p| p.tracks)
                            .flat_map(|t| t.uri)
                            .collect(),
                    ),
                    None => Err(SpircError::NoUri("context"))?,
                };

                let context_options = play
                    .options
                    .player_options_override
                    .map(Into::into)
                    .map(LoadContextOptions::Options);

                let fallback_index = play
                    .options
                    .skip_to
                    .as_ref()
                    .and_then(|s| s.track_index)
                    .map(|i| i as usize);

                self.handle_load(
                    LoadRequest {
                        context,
                        options: LoadRequestOptions {
                            start_playing: true,
                            seek_to: play.options.seek_to.unwrap_or_default(),
                            playing_track: play.options.skip_to.and_then(|s| s.try_into().ok()),
                            context_options,
                        },
                    },
                    play.context.pages.pop(),
                    fallback_index,
                )
                .await?;

                self.connect_state.set_origin(play.play_origin)
            }
            Pause(_) => self.handle_pause(),
            SeekTo(seek_to) => {
                // for some reason the position is stored in value, not in position
                trace!("seek to {seek_to:?}");
                self.handle_seek(seek_to.value)
            }
            SetShufflingContext(shuffle) => self.handle_shuffle(shuffle.value)?,
            SetRepeatingContext(repeat_context) => {
                self.handle_repeat_context(repeat_context.value)?
            }
            SetRepeatingTrack(repeat_track) => self.handle_repeat_track(repeat_track.value),
            AddToQueue(add_to_queue) => self.connect_state.add_to_queue(add_to_queue.track, true),
            SetQueue(set_queue) => self.connect_state.handle_set_queue(set_queue),
            SetOptions(set_options) => {
                if let Some(repeat_context) = set_options.repeating_context {
                    self.handle_repeat_context(repeat_context)?
                }

                if let Some(repeat_track) = set_options.repeating_track {
                    self.handle_repeat_track(repeat_track)
                }

                let shuffle = set_options.shuffling_context;
                if let Some(shuffle) = shuffle {
                    self.handle_shuffle(shuffle)?;
                }
            }
            SkipNext(skip_next) => self.handle_next(skip_next.track.map(|t| t.uri))?,
            SkipPrev(_) => self.handle_prev()?,
            Resume(_) if matches!(self.play_status, SpircPlayStatus::Stopped) => {
                self.load_track(true, 0)?
            }
            Resume(_) => self.handle_play(),
        }

        self.update_state = true;
        Ok(())
    }

    fn handle_transfer(&mut self, mut transfer: TransferState) -> Result<(), Error> {
        self.pending_end_of_track = None;
        // A transfer replaces one still being set up; finishing that one later,
        // or its resolve replacing the context, would undo this one.
        if self.transfer_state.take().is_some() {
            self.context_resolver.clear();
        }
        let mut ctx_uri = match transfer.current_session.context.uri {
            // can apparently happen when a state is transferred and was started with "uris" via
            // the api, or when the source device has no session left (an idle phone): fall back
            // to the transferred track
            None => None,
            Some(ref uri) if uri == "-" || uri.is_empty() => None,
            Some(ref uri) => Some(uri.clone()),
        };

        self.connect_state.reset_context(
            ctx_uri
                .as_deref()
                .map(ResetContext::WhenDifferent)
                .unwrap_or(ResetContext::Completely),
        );

        match self.connect_state.current_track_from_transfer(&transfer) {
            Err(why) => warn!("didn't find initial track: {why}"),
            Ok(track) => {
                debug!("found initial track <{}>", track.uri);
                self.connect_state.set_track(track)
            }
        };

        let autoplay = self.connect_state.current_track(|t| t.is_autoplay());
        if autoplay {
            ctx_uri = ctx_uri.map(|c| c.replace("station:", ""));
        }

        let fallback = self.connect_state.current_track(|t| &t.uri).clone();
        let mut load_from_context_uri = ctx_uri.is_some();

        match ctx_uri {
            Some(ref uri) => {
                self.context_resolver.add(ResolveContext::from_uri(
                    uri.clone(),
                    &fallback,
                    ContextType::Default,
                    ContextAction::Replace,
                ));
            }
            None => {
                let all_tracks = transfer
                    .current_session
                    .context
                    .pages
                    .iter()
                    .cloned()
                    .flat_map(|p| p.tracks)
                    .collect::<Vec<_>>();

                if !all_tracks.is_empty() {
                    self.load_context_from_tracks(all_tracks)?;
                } else if !fallback.is_empty() {
                    // No context and no tracks, but a track: resolve the track as
                    // its own context, so the transfer finishes and autoplay follows.
                    warn!(
                        "tried to transfer with an invalid state, using fallback as ctx_uri ({fallback})"
                    );
                    self.context_resolver.add(ResolveContext::from_uri(
                        fallback.clone(),
                        &fallback,
                        ContextType::Default,
                        ContextAction::Replace,
                    ));
                    ctx_uri = Some(fallback.clone());
                    load_from_context_uri = true;
                } else {
                    warn!("the transfer carried no context and no track, nothing to continue");
                }
            }
        };

        self.handle_activate();

        let timestamp = self.now_ms();
        let state = &mut self.connect_state;
        state.handle_initial_transfer(&mut transfer, ctx_uri.clone());

        // adjust active context, so resolve knows for which context it should set up the state
        state.active_context = if autoplay {
            ContextType::Autoplay
        } else {
            ContextType::Default
        };

        // update position if the track continued playing
        let position = transfer_position(&transfer.playback, timestamp);

        let is_playing = !transfer.playback.is_paused();

        if self.connect_state.current_track(|t| t.is_autoplay()) || autoplay {
            if let Some(ctx_uri) = ctx_uri {
                debug!("currently in autoplay context, async resolving autoplay for {ctx_uri}");
                self.context_resolver.add(ResolveContext::from_uri(
                    ctx_uri,
                    fallback,
                    ContextType::Autoplay,
                    ContextAction::Replace,
                ))
            } else {
                warn!("couldn't resolve autoplay context without a context uri");
            }
        }

        // The transfer finishes when its context resolves; a resolve the resolver
        // declined (a context it marked unavailable) will never finish it.
        if load_from_context_uri {
            if self.context_resolver.has_next() {
                self.transfer_state = Some(transfer);
            }
        } else {
            match self.connect_state.get_context(ContextType::Default) {
                Err(why) => {
                    warn!("continuing transfer in an unknown state. {why}");
                    // Only a resolve still to come can finish the transfer; without
                    // one the state would linger and be applied over later playback.
                    if self.context_resolver.has_next() {
                        self.transfer_state = Some(transfer);
                    }
                }
                Ok(ctx) => {
                    let idx = ConnectState::find_index_in_context(ctx, |pt| {
                        self.connect_state.current_track(|t| pt.uri == t.uri)
                    })?;
                    self.connect_state.reset_playback_to_position(Some(idx))?;
                }
            }
        }

        self.load_track(is_playing, position)
    }

    async fn handle_disconnect(&mut self) -> Result<(), Error> {
        self.context_resolver.clear();
        // Nothing will finish setting up a transfer or continue a parked track.
        self.transfer_state = None;
        self.pending_end_of_track = None;

        self.play_status = SpircPlayStatus::Stopped {};
        self.connect_state
            .update_position_in_relation(self.now_ms());
        self.notify().await?;

        self.connect_state.became_inactive(&self.session).await?;

        self.player
            .emit_session_disconnected_event(self.session.connection_id(), self.session.username());

        Ok(())
    }

    fn handle_stop(&mut self) {
        self.player.stop();
        self.connect_state.update_position(0, self.now_ms());
        self.connect_state.clear_next_tracks();

        if let Err(why) = self.connect_state.reset_playback_to_position(None) {
            warn!("failed filling up next_track during stopping: {why}")
        }
    }

    /// Who sent a command: a device from the cluster, a Web API app
    /// (`webapi-<client id>`), or nobody remote (`local`, a command from here).
    /// `Some(None)` for a command from here, `Some(Some(_))` for a named remote
    /// controller, `None` for a device the device list doesn't name yet.
    fn controller_for(&self, sender: &str) -> Option<Option<Controller>> {
        if sender.is_empty() || sender == "local" || sender == self.session.device_id() {
            return Some(None);
        }
        if let Some(device) = self.devices.get(sender) {
            return Some(Some(Controller {
                client_id: device.client_id.clone(),
                name: device.name.clone(),
                brand: device.brand.clone(),
                model: device.model.clone(),
            }));
        }
        sender.strip_prefix("webapi-").map(|client_id| {
            Some(Controller {
                client_id: client_id.to_string(),
                ..Default::default()
            })
        })
    }

    fn set_controller_from(&mut self, sender: &str) {
        match self.controller_for(sender) {
            Some(controller) => {
                self.controller = controller;
                self.unresolved_sender = None;
            }
            None => {
                // Not the previous controller either: report no remote one
                // until the device list names this sender.
                self.controller = None;
                self.unresolved_sender = Some(sender.to_string());
            }
        }
    }

    /// Emits session_client_changed for the current controller, when it differs
    /// from the last one reported or when `always`.
    fn report_client(&mut self, always: bool) {
        if self.unresolved_sender.is_some() && !always {
            // Reported with its name once the device list has it.
            return;
        }
        // Without a remote controller, report this session's own client (the
        // session's client id is ours and authenticates us; never overwrite it).
        let client = self.controller.clone().unwrap_or_else(|| Controller {
            client_id: self.session.client_id(),
            name: self.session.client_name(),
            brand: self.session.client_brand_name(),
            model: self.session.client_model_name(),
        });
        if !always && self.reported_client.as_ref() == Some(&client) {
            return;
        }
        self.player.emit_session_client_changed_event(
            client.client_id.clone(),
            client.name.clone(),
            client.brand.clone(),
            client.model.clone(),
        );
        self.reported_client = Some(client);
    }

    fn handle_activate(&mut self) {
        self.connect_state.set_active(true);
        self.player
            .emit_session_connected_event(self.session.connection_id(), self.session.username());
        self.report_client(true);

        self.player
            .emit_volume_changed_event(self.connect_state.device_info().volume as u16);

        self.player
            .emit_auto_play_changed_event(self.session.autoplay());

        self.player
            .emit_filter_explicit_content_changed_event(self.session.filter_explicit_content());

        self.player
            .emit_shuffle_changed_event(self.connect_state.shuffling_context());

        self.player.emit_repeat_changed_event(
            self.connect_state.repeat_context(),
            self.connect_state.repeat_track(),
        );
    }

    async fn handle_load(
        &mut self,
        cmd: LoadRequest,
        page: Option<ContextPage>,
        fallback_index: Option<usize>,
    ) -> Result<(), Error> {
        self.pending_end_of_track = None;
        // A load replaces a transfer still being set up; finishing that transfer,
        // or its resolve replacing the context, would undo the load.
        if self.transfer_state.take().is_some() {
            self.context_resolver.clear();
        }
        self.connect_state
            .reset_context(if let PlayContext::Uri(ref uri) = cmd.context {
                ResetContext::WhenDifferent(uri)
            } else {
                ResetContext::Completely
            });

        self.connect_state.reset_options();

        let autoplay = matches!(cmd.context_options, Some(LoadContextOptions::Autoplay));
        match cmd.context {
            PlayContext::Uri(uri) => {
                self.load_context_from_uri(uri, page.as_ref(), autoplay)
                    .await?
            }
            PlayContext::Tracks(tracks) => self.load_context_from_tracks(tracks)?,
        }

        let cmd_options = cmd.options;

        self.connect_state.set_active_context(ContextType::Default);

        // for play commands with skip by uid, the context of the command contains
        // tracks with uri and uid, so we merge the new context with the resolved/existing context
        self.connect_state.merge_context(page);

        // load here, so that we clear the queue only after we definitely retrieved a new context
        self.connect_state.clear_next_tracks();
        self.connect_state.clear_restrictions();

        debug!("play track <{:?}>", cmd_options.playing_track);

        let index = match cmd_options.playing_track {
            None => None,
            Some(ref playing_track) => Some(match playing_track {
                PlayingTrack::Index(i) => Ok(*i as usize),
                PlayingTrack::Uri(uri) => {
                    let ctx = self.connect_state.get_context(ContextType::Default)?;
                    ConnectState::find_index_in_context(ctx, |t| &t.uri == uri)
                }
                PlayingTrack::Uid(uid) => {
                    let ctx = self.connect_state.get_context(ContextType::Default)?;
                    ConnectState::find_index_in_context(ctx, |t| &t.uid == uid)
                }
            }),
        }
        .map(|i| {
            i.unwrap_or_else(|why| {
                warn!(
                    "Failed to resolve index by {:?}, using fallback index: {:?} (Error: {why})",
                    cmd_options.playing_track, fallback_index
                );
                fallback_index.unwrap_or_default()
            })
        });

        if let Some(LoadContextOptions::Options(ref options)) = cmd_options.context_options {
            debug!(
                "loading with shuffle: <{}>, repeat track: <{}> context: <{}>",
                options.shuffle, options.repeat, options.repeat_track
            );

            self.connect_state.set_shuffle(options.shuffle);
            self.connect_state.set_repeat_context(options.repeat);
            self.connect_state.set_repeat_track(options.repeat_track);
        }

        if matches!(cmd_options.context_options, Some(LoadContextOptions::Options(ref o)) if o.shuffle)
        {
            if let Some(index) = index {
                self.connect_state.set_current_track(index)?;
            } else {
                self.connect_state.set_current_track_random()?;
            }

            if self.context_resolver.has_next() {
                self.connect_state.update_queue_revision()
            } else {
                self.connect_state.shuffle_new()?;
                self.add_autoplay_resolving_when_required();
            }
        } else {
            self.connect_state
                .set_current_track(index.unwrap_or_default())?;
            self.connect_state.reset_playback_to_position(index)?;
            self.add_autoplay_resolving_when_required();
        }

        if self.connect_state.current_track(MessageField::is_some) {
            self.load_track(cmd_options.start_playing, cmd_options.seek_to)?;
        } else {
            info!("No active track, stopping");
            self.handle_stop()
        }

        Ok(())
    }

    async fn load_context_from_uri(
        &mut self,
        context_uri: String,
        page: Option<&ContextPage>,
        autoplay: bool,
    ) -> Result<(), Error> {
        if !self.connect_state.is_active() {
            self.handle_activate();
        }

        let update_context = if autoplay {
            ContextType::Autoplay
        } else {
            ContextType::Default
        };

        self.connect_state.set_active_context(update_context);

        let fallback = match page {
            // check that the uri is valid or the page has a valid uri that can be used
            Some(page) => match ConnectState::find_valid_uri(Some(&context_uri), Some(page)) {
                Some(ctx_uri) => ctx_uri,
                None => return Err(SpircError::InvalidUri(context_uri).into()),
            },
            // when there is no page, the uri should be valid
            None => &context_uri,
        };

        let current_context_uri = self.connect_state.context_uri();

        if current_context_uri == &context_uri && fallback == context_uri {
            debug!("context <{current_context_uri}> didn't change, no resolving required")
        } else {
            debug!("resolving context for load command");
            self.context_resolver.clear();
            self.context_resolver.add(ResolveContext::from_uri(
                &context_uri,
                fallback,
                update_context,
                ContextAction::Replace,
            ));
            let context = self.context_resolver.get_next_context(Vec::new).await;
            self.handle_next_context(context);
        }

        Ok(())
    }

    fn load_context_from_tracks(&mut self, tracks: impl Into<ContextPage>) -> Result<(), Error> {
        const WEB_API_URI: &str = "spotify:web-api";
        let ctx = Context {
            // by providing values for uri/url the player in the official client's isn't frozen
            uri: Some(WEB_API_URI.into()),
            url: Some(format!("context://{WEB_API_URI}")),
            pages: vec![tracks.into()],
            ..Default::default()
        };

        let _ = self
            .connect_state
            .update_context(ctx, ContextType::Default)?;

        Ok(())
    }

    fn handle_play(&mut self) {
        match self.play_status {
            SpircPlayStatus::Paused {
                position_ms,
                preloading_of_next_track_triggered,
            } => {
                // A track that already ended (past its end on a transfer) waits for
                // its context to move on: don't sound it, just continue playing
                // with whatever follows it.
                if self.pending_end_of_track.is_none() {
                    self.player.play();
                }
                self.connect_state
                    .update_position(position_ms, self.now_ms());
                self.play_status = SpircPlayStatus::Playing {
                    nominal_start_time: self.now_ms() - position_ms as i64,
                    preloading_of_next_track_triggered,
                };
                self.connect_state.set_status(&self.play_status);
            }
            SpircPlayStatus::LoadingPause { position_ms } => {
                self.player.play();
                self.play_status = SpircPlayStatus::LoadingPlay { position_ms };
            }
            _ => return,
        }

        // Synchronize the volume from the mixer. This is useful on
        // systems that can switch sources from and back to librespot.
        let current_volume = self.mixer.volume();
        self.set_volume(current_volume);
    }

    fn handle_play_pause(&mut self) {
        match self.play_status {
            SpircPlayStatus::Paused { .. } | SpircPlayStatus::LoadingPause { .. } => {
                self.handle_play()
            }
            SpircPlayStatus::Playing { .. } | SpircPlayStatus::LoadingPlay { .. } => {
                self.handle_pause()
            }
            _ => (),
        }
    }

    fn handle_pause(&mut self) {
        match self.play_status {
            SpircPlayStatus::Playing {
                nominal_start_time,
                preloading_of_next_track_triggered,
            } => {
                self.player.pause();
                let position_ms = (self.now_ms() - nominal_start_time) as u32;
                self.connect_state
                    .update_position(position_ms, self.now_ms());
                self.play_status = SpircPlayStatus::Paused {
                    position_ms,
                    preloading_of_next_track_triggered,
                };
            }
            SpircPlayStatus::LoadingPlay { position_ms } => {
                self.player.pause();
                self.play_status = SpircPlayStatus::LoadingPause { position_ms };
            }
            _ => (),
        }
    }

    fn handle_seek(&mut self, position_ms: u32) {
        let duration = self.connect_state.player().duration;
        if i64::from(position_ms) > duration {
            warn!("tried to seek to {position_ms}ms of {duration}ms");
            return;
        }

        // The track already ended and the player has nothing to seek: load it
        // again at the requested position.
        if self.pending_end_of_track.take().is_some() {
            if let Err(why) = self.load_track(self.connect_state.is_playing(), position_ms) {
                warn!("couldn't restart the ended track: {why}");
            }
            return;
        }

        self.connect_state
            .update_position(position_ms, self.now_ms());
        self.player.seek(position_ms);
        let now = self.now_ms();
        match self.play_status {
            SpircPlayStatus::Stopped => (),
            SpircPlayStatus::LoadingPause {
                position_ms: ref mut position,
            }
            | SpircPlayStatus::LoadingPlay {
                position_ms: ref mut position,
            }
            | SpircPlayStatus::Paused {
                position_ms: ref mut position,
                ..
            } => *position = position_ms,
            SpircPlayStatus::Playing {
                ref mut nominal_start_time,
                ..
            } => *nominal_start_time = now - position_ms as i64,
        };
    }

    fn handle_shuffle(&mut self, shuffle: bool) -> Result<(), Error> {
        self.player.emit_shuffle_changed_event(shuffle);
        self.connect_state.handle_shuffle(shuffle)
    }

    fn handle_repeat_context(&mut self, repeat: bool) -> Result<(), Error> {
        self.player
            .emit_repeat_changed_event(repeat, self.connect_state.repeat_track());
        self.connect_state.handle_set_repeat_context(repeat)
    }

    fn handle_repeat_track(&mut self, repeat: bool) {
        self.player
            .emit_repeat_changed_event(self.connect_state.repeat_context(), repeat);
        self.connect_state.set_repeat_track(repeat);
    }

    fn handle_preload_next_track(&mut self) {
        // Requests the player thread to preload the next track
        match self.play_status {
            SpircPlayStatus::Paused {
                ref mut preloading_of_next_track_triggered,
                ..
            }
            | SpircPlayStatus::Playing {
                ref mut preloading_of_next_track_triggered,
                ..
            } => {
                *preloading_of_next_track_triggered = true;
            }
            _ => (),
        }

        if let Some(track_id) = self.connect_state.preview_next_track() {
            self.player.preload(track_id);
        }
    }

    // Mark unavailable tracks so we can skip them later
    fn handle_unavailable(&mut self, track_id: &SpotifyUri) -> Result<(), Error> {
        self.connect_state.mark_unavailable(track_id)?;
        self.handle_preload_next_track();

        Ok(())
    }

    fn add_autoplay_resolving_when_required(&mut self) {
        let require_load_new = !self
            .connect_state
            .has_next_tracks(Some(CONTEXT_FETCH_THRESHOLD))
            && self.session.autoplay()
            && !self.connect_state.context_uri().is_empty();

        if !require_load_new {
            return;
        }

        let current_context = self.connect_state.context_uri();
        let fallback = self.connect_state.current_track(|t| &t.uri);

        let has_tracks = self
            .connect_state
            .get_context(ContextType::Autoplay)
            .map(|c| !c.tracks.is_empty())
            .unwrap_or_default();

        let resolve = ResolveContext::from_uri(
            current_context,
            fallback,
            ContextType::Autoplay,
            if has_tracks {
                ContextAction::Append
            } else {
                ContextAction::Replace
            },
        );

        self.context_resolver.add(resolve);
    }

    fn handle_next(&mut self, track_uri: Option<String>) -> Result<(), Error> {
        // The user moved on: a track that ended during a transfer is settled.
        self.pending_end_of_track = None;
        let continue_playing = self.connect_state.is_playing();

        let current_uri = self.connect_state.current_track(|t| &t.uri);
        let mut has_next_track =
            matches!(track_uri, Some(ref track_uri) if current_uri == track_uri);

        if !has_next_track {
            has_next_track = loop {
                let index = self.connect_state.next_track()?;

                let current_uri = self.connect_state.current_track(|t| &t.uri);
                if matches!(track_uri, Some(ref track_uri) if current_uri != track_uri) {
                    continue;
                } else {
                    break index.is_some();
                }
            };
        };

        if has_next_track {
            self.add_autoplay_resolving_when_required();
            self.load_track(continue_playing, 0)
        } else {
            info!("Not playing next track because there are no more tracks left in queue.");
            self.handle_stop();
            Ok(())
        }
    }

    fn handle_clear_queue(&mut self) {
        self.connect_state.clear_queue();
        if let Err(why) = self.connect_state.fill_up_next_tracks() {
            warn!("failed filling up next_track after clearing the queue: {why}")
        }
    }

    fn handle_add_to_queue(&mut self, uri: String) {
        let track = librespot_protocol::player::ProvidedTrack {
            uri,
            ..Default::default()
        };
        self.connect_state.add_to_queue(track, true);
    }

    fn handle_prev(&mut self) -> Result<(), Error> {
        // Previous behaves differently based on the position
        // Under 3s it goes to the previous song (starts playing)
        // Over 3s it seeks to zero (retains previous play status)
        if self.position() < 3000 {
            let repeat_context = self.connect_state.repeat_context();
            match self.connect_state.prev_track()? {
                None if repeat_context => self.connect_state.reset_playback_to_position(None)?,
                None => {
                    self.pending_end_of_track = None;
                    self.connect_state.reset_playback_to_position(None)?;
                    self.handle_stop()
                }
                Some(_) => {
                    // A new track settles any track that ended during a transfer.
                    self.pending_end_of_track = None;
                    self.load_track(self.connect_state.is_playing(), 0)?
                }
            }
        } else if self.pending_end_of_track.take().is_some() {
            // The track already ended (past its end on a transfer) and the player
            // has nothing to seek: restart it by loading it again.
            self.load_track(self.connect_state.is_playing(), 0)?;
        } else {
            self.handle_seek(0);
        }

        Ok(())
    }

    fn handle_volume_up(&mut self) {
        let volume = (self.connect_state.device_info().volume as u16)
            .saturating_add(self.connect_state.volume_step_size);

        self.set_volume(volume);
    }

    fn handle_volume_down(&mut self) {
        let volume = (self.connect_state.device_info().volume as u16)
            .saturating_sub(self.connect_state.volume_step_size);

        self.set_volume(volume);
    }

    fn handle_playlist_modification(
        &mut self,
        playlist_modification_info: PlaylistModificationInfo,
    ) -> Result<(), Error> {
        let uri = playlist_modification_info
            .uri
            .ok_or(SpircError::NoUri("playlist modification"))?;
        let uri = String::from_utf8(uri)?;

        if self.connect_state.context_uri() != &uri {
            debug!(
                "ignoring playlist modification update for playlist <{uri}>, because it isn't the current context"
            );
            return Ok(());
        }

        debug!("playlist modification for current context: {uri}");
        self.context_resolver.add(ResolveContext::from_uri(
            uri,
            self.connect_state.current_track(|t| &t.uri),
            ContextType::Default,
            ContextAction::Replace,
        ));

        Ok(())
    }

    fn handle_session_update(&mut self, session_update: FallbackWrapper<SessionUpdate>) {
        // we know that this enum value isn't present in our current proto definitions, by that
        // the json parsing fails because the enum isn't known as proto representation
        const WBC: &str = "WIFI_BROADCAST_CHANGED";

        let mut session_update = match session_update {
            FallbackWrapper::Inner(update) => update,
            FallbackWrapper::Fallback(value) => {
                let fallback_inner = value.to_string();
                if fallback_inner.contains(WBC) {
                    log::debug!("Received SessionUpdate::{WBC}");
                } else {
                    log::warn!("SessionUpdate couldn't be parse correctly: {value:?}");
                }
                return;
            }
        };

        let reason = session_update.reason.enum_value();

        let mut session = match session_update.session.take() {
            None => return,
            Some(session) => session,
        };

        let active_device = session.host_active_device_id.take();
        if matches!(active_device, Some(ref device) if device == self.session.device_id()) {
            info!(
                "session update: <{:?}> for self, current session_id {}, new session_id {}",
                reason,
                self.session.session_id(),
                session.session_id
            );

            if self.session.session_id() != session.session_id {
                self.session.set_session_id(&session.session_id);
                self.connect_state.set_session_id(session.session_id);
            }
        } else {
            debug!("session update: <{reason:?}> from active session host: <{active_device:?}>");
        }

        // this seems to be used for jams or handling the current session_id
        //
        // handling this event was intended to keep the playback when other clients (primarily
        // mobile) connects, otherwise they would steel the current playback when there was no
        // session_id provided on the initial PutStateReason::NEW_DEVICE state update
        //
        // by generating an initial session_id from the get-go we prevent that behavior and
        // currently don't need to handle this event, might still be useful for later "jam" support
    }

    fn position(&mut self) -> u32 {
        match self.play_status {
            SpircPlayStatus::Stopped => 0,
            SpircPlayStatus::LoadingPlay { position_ms }
            | SpircPlayStatus::LoadingPause { position_ms }
            | SpircPlayStatus::Paused { position_ms, .. } => position_ms,
            SpircPlayStatus::Playing {
                nominal_start_time, ..
            } => (self.now_ms() - nominal_start_time) as u32,
        }
    }

    fn load_track(&mut self, start_playing: bool, position_ms: u32) -> Result<(), Error> {
        if self.connect_state.current_track(MessageField::is_none) {
            debug!("current track is none, stopping playback");
            self.handle_stop();
            return Ok(());
        }

        let current_uri = self.connect_state.current_track(|t| &t.uri);
        let id = SpotifyUri::from_uri(current_uri)?;
        self.player.load(id, start_playing, position_ms);

        self.connect_state
            .update_position(position_ms, self.now_ms());
        if start_playing {
            self.play_status = SpircPlayStatus::LoadingPlay { position_ms };
        } else {
            self.play_status = SpircPlayStatus::LoadingPause { position_ms };
        }
        self.connect_state.set_status(&self.play_status);

        Ok(())
    }

    async fn notify(&mut self) -> Result<(), Error> {
        self.connect_state.set_status(&self.play_status);

        if self.connect_state.is_playing() {
            self.connect_state
                .update_position_in_relation(self.now_ms());
        }

        self.connect_state.set_now(self.now_ms() as u64);

        self.connect_state
            .send_state(&self.session)
            .await
            .map(|_| ())
    }

    fn set_volume(&mut self, volume: u16) {
        debug!("SpircTask::set_volume({volume})");

        let old_volume = self.connect_state.device_info().volume;
        let new_volume = volume as u32;
        if old_volume != new_volume || self.mixer.volume() != volume {
            self.update_volume = true;

            self.connect_state.set_volume(new_volume);
            self.mixer.set_volume(volume);
            if let Some(cache) = self.session.cache() {
                cache.save_volume(volume)
            }
            if self.connect_state.is_active() {
                self.player.emit_volume_changed_event(volume);
            }
        }
    }
}

impl Drop for SpircTask {
    fn drop(&mut self) {
        debug!("drop Spirc[{}]", self.spirc_id);
    }
}

#[cfg(test)]
mod recovery_tests {
    mod transfer_position {
        use super::super::transfer_position;
        use librespot_protocol::playback::Playback;

        fn playback(position: i32, timestamp: i64, speed: Option<f64>, paused: bool) -> Playback {
            Playback {
                position_as_of_timestamp: Some(position),
                timestamp: Some(timestamp),
                playback_speed: speed,
                is_paused: Some(paused),
                ..Default::default()
            }
        }

        #[test]
        fn a_playing_track_moves_on_from_where_it_was_published() {
            assert_eq!(
                transfer_position(&playback(60_000, 1_000, Some(1.0), false), 6_000),
                65_000
            );
        }

        #[test]
        fn a_track_published_at_zero_still_moves_on() {
            assert_eq!(
                transfer_position(&playback(0, 1_000, Some(1.0), false), 12_000),
                11_000
            );
        }

        #[test]
        fn paused_or_speed_zero_or_missing_speed_stays_put() {
            assert_eq!(
                transfer_position(&playback(60_000, 1_000, Some(1.0), true), 9_000),
                60_000
            );
            assert_eq!(
                transfer_position(&playback(60_000, 1_000, Some(0.0), false), 9_000),
                60_000
            );
            assert_eq!(
                transfer_position(&playback(60_000, 1_000, None, false), 9_000),
                60_000
            );
        }

        #[test]
        fn a_missing_timestamp_is_not_extrapolated_from_the_epoch() {
            assert_eq!(
                transfer_position(&playback(5_000, 0, Some(1.0), false), 1_790_000_000_000),
                5_000
            );
        }

        #[test]
        fn out_of_range_data_is_clamped_not_failed() {
            assert_eq!(
                transfer_position(&playback(-5, 1_000, Some(1.0), true), 2_000),
                0
            );
            assert_eq!(
                transfer_position(&playback(0, 1_000, Some(-3.0), false), 9_000),
                0
            );
            assert_eq!(
                transfer_position(&playback(i32::MAX, 1, Some(f64::MAX), false), i64::MAX),
                u32::MAX
            );
            // A clock behind the publisher's doesn't move the position back.
            assert_eq!(
                transfer_position(&playback(60_000, 9_000, Some(1.0), false), 1_000),
                60_000
            );
        }
    }

    use super::*;

    #[tokio::test]
    async fn recovery_keeps_unresolved_pages_and_rejects_another_account() {
        let session = Session::new(Default::default(), None);
        let mut old = ConnectState::new(Default::default(), &session);
        old.set_repeat_context(true);
        let pending = ResolveContext::from_uri(
            "spotify:playlist:remaining",
            "",
            ContextType::Default,
            ContextAction::Append,
        );
        let snapshot = PlaybackSnapshot {
            state: old,
            pending: [pending.clone()].into(),
            username: "original".into(),
            position_ms: 144_075,
            playing: false,
        };
        let mut state = ConnectState::new(Default::default(), &session);
        let mut resolver = ContextResolver::new(session);
        assert!(
            snapshot
                .restore_into("different", &mut state, &mut resolver)
                .is_err()
        );
        assert!(!state.repeat_context());
        assert!(resolver.pending().is_empty());
        snapshot
            .restore_into("original", &mut state, &mut resolver)
            .unwrap();
        assert!(state.repeat_context());
        assert_eq!(resolver.pending(), [pending]);
    }
}
