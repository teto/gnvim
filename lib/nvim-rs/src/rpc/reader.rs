use std::{collections::VecDeque, io};

use futures::prelude::*;

use super::message::Message;

/// Cursor implementing non-destructive read for `VecDeque`.
pub(crate) struct Cursor<'a> {
    inner: &'a VecDeque<u8>,
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(deque: &'a VecDeque<u8>) -> Self {
        Self {
            inner: deque,
            pos: 0,
        }
    }
}

impl io::Read for Cursor<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let start = self.pos.min(self.inner.len());
        let mut read = 0;

        let (front, back) = self.inner.as_slices();

        // Read from front slice.
        if start < front.len() {
            let f = &front[start..];
            let n = f.len().min(buf.len());
            buf[..n].copy_from_slice(&f[..n]);
            read += n;
        }

        // If there's still space in buf, read from back slice.
        if read < buf.len() && start + read >= front.len() {
            let b_start = start + read - front.len();
            if b_start < back.len() {
                let b = &back[b_start..];
                let n = b.len().min(buf.len() - read);
                buf[read..read + n].copy_from_slice(&b[..n]);
                read += n;
            }
        }

        self.pos += read;
        Ok(read)
    }
}

#[derive(Debug)]
pub enum ReadError {
    IOError(io::Error),
    RmpError(rmp_serde::decode::Error),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::IOError(err) => f.write_fmt(format_args!("io error: {}", err)),
            ReadError::RmpError(err) => f.write_fmt(format_args!("rmp error: {}", err)),
        }
    }
}

pub struct RpcReader<R>
where
    R: AsyncRead + Unpin,
{
    reader: futures::io::BufReader<R>,
    buf: VecDeque<u8>,
}

impl<R> RpcReader<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(reader: R) -> Self {
        Self {
            reader: futures::io::BufReader::new(reader),
            buf: VecDeque::new(),
        }
    }

    pub fn into_inner(self) -> R {
        self.reader.into_inner()
    }

    async fn fill_buffer(&mut self) -> Result<(), ReadError> {
        match self.reader.fill_buf().await {
            Ok([]) => Err(ReadError::IOError(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Read zero bytes",
            ))),
            Ok(bytes) => {
                // Add the available bytes to our buffer.
                self.buf.extend(bytes);

                // Tell the reader that we consumed the values.
                let len = bytes.len();
                self.reader.consume_unpin(len);
                Ok(())
            }
            Err(err) => Err(ReadError::IOError(err)),
        }
    }

    pub async fn recv(&mut self) -> Result<Message, ReadError> {
        loop {
            let mut cursor = Cursor::new(&self.buf);

            // Try decoding value from the buffer's current content.
            match rmp_serde::from_read::<_, Message>(&mut cursor) {
                Ok(val) => {
                    // All good, there was enough data. Drop the read data.
                    self.buf.drain(..cursor.pos);

                    return Ok(val);
                }
                // If we got an UnexpectedEof error, try reading more data into
                // the buffer.
                Err(rmp_serde::decode::Error::InvalidMarkerRead(err))
                | Err(rmp_serde::decode::Error::InvalidDataRead(err))
                    if err.kind() == io::ErrorKind::UnexpectedEof =>
                {
                    self.fill_buffer().await?
                }
                Err(err) => {
                    return Err(ReadError::RmpError(err));
                }
            }
        }
    }
}

impl<R> From<R> for RpcReader<R>
where
    R: AsyncRead + Unpin,
{
    fn from(r: R) -> Self {
        RpcReader::new(r)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        io::{Read, Write},
    };

    use super::Cursor;
    #[test]
    fn test_reads_front_and_back_at_once() {
        let mut dq: VecDeque<u8> = VecDeque::from_iter(0..6);
        dq.pop_front();
        dq.pop_front();
        dq.extend(6..8);

        // Validate our test setup.
        let (front, back) = dq.as_slices();
        assert_eq!(front, &[2, 3, 4, 5]);
        assert_eq!(back, &[6, 7]);

        let mut cursor = Cursor::new(&mut dq);

        let mut buf = vec![0u8; 6];
        let n = cursor.read(&mut buf).unwrap();
        assert_eq!(n, 6);
        assert_eq!(&buf, &[2, 3, 4, 5, 6, 7])
    }

    #[test]
    fn test_cursor_read_wrapping() {
        let mut buf = VecDeque::with_capacity(5);
        buf.write_all(&[1, 2, 3, 4, 5]).unwrap();
        buf.drain(..2);
        buf.write_all(&[6]).unwrap();

        // Buffer should be [6, <empty>, 3, 4, 5]
        let (front, back) = buf.as_slices();
        assert_eq!(&[3, 4, 5], front);
        assert_eq!(&[6], back);

        let mut cursor = Cursor::new(&buf);
        let mut target = Vec::new();
        let n = cursor.read_to_end(&mut target).unwrap();
        assert_eq!(4, n);
        assert_eq!(vec![3, 4, 5, 6], target);
    }
}
