// SPDX-License-Identifier: Apache-2.0
//! Disposable native qualification fixture for account and elevation boundaries.

#[cfg(not(windows))]
fn main() {
    eprintln!("pipe peer qualification requires native Windows");
    std::process::exit(2);
}

#[cfg(windows)]
fn main() {
    use std::path::PathBuf;
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    assert_eq!(args.len(), 3, "MODE ENDPOINT REPORT");
    let mode = args[0].to_str().expect("fixture mode");
    let endpoint = PathBuf::from(&args[1]);
    let report = PathBuf::from(&args[2]);
    let result = native::run(mode, &endpoint);
    let (code, payload) = match result {
        Ok(peer) => (
            0,
            format!(
                "{{\"ok\":true,\"pid\":{},\"peerPid\":{peer}}}\n",
                std::process::id()
            ),
        ),
        Err(error) => (
            1,
            format!(
                "{{\"ok\":false,\"pid\":{},\"errorKind\":\"{:?}\",\"winerror\":{}}}\n",
                std::process::id(),
                error.kind(),
                error.raw_os_error().unwrap_or(0)
            ),
        ),
    };
    std::fs::write(report, payload).expect("fixture report");
    std::process::exit(code);
}

#[cfg(windows)]
mod native {
    use std::io::{self, Read, Write};
    use std::path::Path;
    use std::time::{Duration, Instant};
    use tokio::net::windows::named_pipe::ClientOptions;
    use undercurrent_platform_windows::{
        BlockingPipeListener, BlockingPipeStream, pipe_name, verify_pipe_server,
    };
    use windows_sys::Win32::Storage::FileSystem::SECURITY_ANONYMOUS;

    pub fn run(mode: &str, endpoint: &Path) -> io::Result<u32> {
        match mode {
            "server" => serve(endpoint),
            "client" => client(endpoint),
            "anonymous" => tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(anonymous(endpoint)),
            _ => Err(io::Error::new(io::ErrorKind::InvalidInput, "fixture mode")),
        }
    }

    fn serve(endpoint: &Path) -> io::Result<u32> {
        let listener = BlockingPipeListener::bind(endpoint)?;
        listener.set_nonblocking(true)?;
        std::fs::write(endpoint.with_extension("ready"), b"ready")?;
        let deadline = Instant::now() + Duration::from_secs(20);
        let (mut stream, ()) = loop {
            match listener.accept() {
                Ok(connection) => break connection,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err(io::ErrorKind::TimedOut.into());
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => return Err(error),
            }
        };
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let peer = stream.peer_process_id()?;
        let mut byte = [0];
        stream.read_exact(&mut byte)?;
        if byte != *b"P" {
            return Err(io::ErrorKind::InvalidData.into());
        }
        stream.write_all(b"K")?;
        // Retain the verified peer until the client consumes its response.
        let _ = stream.read(&mut byte);
        Ok(peer)
    }

    fn client(endpoint: &Path) -> io::Result<u32> {
        let mut stream = BlockingPipeStream::connect(endpoint)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let peer = stream.peer_process_id()?;
        stream.write_all(b"P")?;
        let mut byte = [0];
        stream.read_exact(&mut byte)?;
        if byte != *b"K" {
            return Err(io::ErrorKind::InvalidData.into());
        }
        Ok(peer)
    }

    async fn anonymous(endpoint: &Path) -> io::Result<u32> {
        let pipe = ClientOptions::new()
            .security_qos_flags(SECURITY_ANONYMOUS)
            .open(pipe_name(endpoint)?)?;
        verify_pipe_server(&pipe)?;
        loop {
            pipe.writable().await?;
            match pipe.try_write(b"P") {
                Ok(1) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                _ => return Err(io::ErrorKind::BrokenPipe.into()),
            }
        }
        let mut byte = [0];
        loop {
            pipe.readable().await?;
            match pipe.try_read(&mut byte) {
                Ok(1) if byte == *b"K" => return Ok(0),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Err(error) => return Err(error),
                _ => return Err(io::ErrorKind::InvalidData.into()),
            }
        }
    }
}
