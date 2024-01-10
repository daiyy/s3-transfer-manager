use std::pin::Pin;
use std::task::Poll;
use bytes::Bytes;
use aws_smithy_types::byte_stream::{ByteStream, error::Error};

pub(crate) struct ByteStreamProgress<'a> {
    inner: ByteStream,
    progress_callback: Option<&'a dyn Fn(usize)>,
}

impl<'a> ByteStreamProgress<'a> {
    pub fn new(stream: ByteStream, progress_callback: Option<&'a dyn Fn(usize)>) -> Self {
        Self {
            inner: stream,
            progress_callback: progress_callback,
        }
    }

    #[allow(dead_code)]
    pub async fn next(&mut self) -> Option<Result<Bytes, Error>> {
        let res = self.inner.next().await;
        if let Some(Ok(ref bytes)) = res {
            if let Some(cb) = &self.progress_callback {
                (cb)(bytes.len());
            }
        }
        return res;
    }

    pub fn into_async_read(self) -> impl tokio::io::AsyncBufRead + 'a {
        tokio_util::io::StreamReader::new(self)
    }
}

impl<'a> futures_core::stream::Stream for ByteStreamProgress<'a> {
    type Item = Result<Bytes, Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Option<Self::Item>> {
        match ByteStream::poll_next(Pin::new(&mut self.inner), cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                if let Some(cb) = &self.progress_callback {
                    (cb)(bytes.len());
                }
                return Poll::Ready(Some(Ok(bytes)));
            },
            Poll::Ready(Some(Err(e))) => {
                return Poll::Ready(Some(Err(e)));
            },
            Poll::Ready(None) => {
                return Poll::Ready(None);
            },
            Poll::Pending => {
                return Poll::Pending;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_update_progress_cb() {
        let stream = ByteStream::from(vec![1,2,3]);
        let f = move |x| { let _ = x; };
        let _ = ByteStreamProgress::new(stream, Some(&f));
    }
}
