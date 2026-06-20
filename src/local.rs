//! Local shell PTY session.
//!
//! Mirrors the SSH/serial/telnet worker shape so the UI can render a local
//! terminal tab through the existing vt100 pipeline.

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{self, UnboundedReceiver};

use crate::i18n::t;
use crate::ssh::{SessionCommand, SessionEvent, SessionHandle};

pub fn spawn_local_session(
    runtime: &Handle,
    tab_id: String,
    initial_cols: u32,
    initial_rows: u32,
) -> (SessionHandle, UnboundedReceiver<SessionEvent>) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<SessionCommand>();
    let (evt_tx, evt_rx) = mpsc::unbounded_channel::<SessionEvent>();

    let evt_tx_for_task = evt_tx.clone();
    let join = runtime.spawn(async move {
        if let Err(err) = tokio::task::spawn_blocking(move || {
            run_local_session(cmd_rx, evt_tx_for_task, initial_cols, initial_rows)
        })
        .await
        {
            let _ = evt_tx.send(SessionEvent::Closed(format!("{err:#}")));
        }
    });

    (
        SessionHandle {
            tab_id,
            commands: cmd_tx,
            join,
        },
        evt_rx,
    )
}

fn run_local_session(
    mut commands: UnboundedReceiver<SessionCommand>,
    events: tokio::sync::mpsc::UnboundedSender<SessionEvent>,
    initial_cols: u32,
    initial_rows: u32,
) -> anyhow::Result<()> {
    let pty_system = native_pty_system();
    let pair = pty_system.openpty(PtySize {
        rows: initial_rows.max(5) as u16,
        cols: initial_cols.max(10) as u16,
        pixel_width: 0,
        pixel_height: 0,
    })?;

    let shell = default_shell();
    let mut child = pair.slave.spawn_command(CommandBuilder::new(&shell))?;
    drop(pair.slave);

    let mut reader = pair.master.try_clone_reader()?;
    let mut writer = pair.master.take_writer()?;
    let master = Arc::new(Mutex::new(pair.master));

    let _ = events.send(SessionEvent::Connected);
    let _ = events.send(SessionEvent::Status(t("本地终端", "Local terminal").into()));

    let reader_events = events.clone();
    let reader_thread = std::thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let text = String::from_utf8_lossy(&buf[..n]).to_string();
                    if reader_events.send(SessionEvent::Output(text)).is_err() {
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
    });

    while let Some(cmd) = commands.blocking_recv() {
        match cmd {
            SessionCommand::RawInput(bytes) => {
                if let Err(err) = writer.write_all(&bytes).and_then(|_| writer.flush()) {
                    let _ = events.send(SessionEvent::Closed(format!(
                        "{}: {err}",
                        t("写入失败", "write failed")
                    )));
                    break;
                }
            }
            SessionCommand::Resize(cols, rows) => {
                if let Ok(master) = master.lock() {
                    let _ = master.resize(PtySize {
                        rows: rows.max(5) as u16,
                        cols: cols.max(10) as u16,
                        pixel_width: 0,
                        pixel_height: 0,
                    });
                }
            }
            SessionCommand::Close => break,
        }
    }

    let _ = child.kill();
    for _ in 0..20 {
        if let Ok(Some(_)) = child.try_wait() {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let _ = reader_thread.join();
    let _ = events.send(SessionEvent::Closed(
        t("本地终端已关闭", "local terminal closed").into(),
    ));
    Ok(())
}

fn default_shell() -> String {
    #[cfg(windows)]
    {
        std::env::var("COMSPEC").unwrap_or_else(|_| "cmd.exe".to_string())
    }
    #[cfg(not(windows))]
    {
        std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string())
    }
}
