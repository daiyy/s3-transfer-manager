use std::path::Path;
use tokio::io::{Error, ErrorKind};
use tokio::io::AsyncReadExt;
use log::trace;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::operation::get_object::GetObjectError;
use crate::uri::S3Uri;
use crate::stream::ByteStreamProgress;

pub struct S3TransferConfig {
    max_concurrency: usize,
    br_threshold: usize,
    mp_threshold: usize,
    mp_chunk_size: usize,
}

impl S3TransferConfig {
    pub fn new() -> Self {
        Self {
            max_concurrency: 1,
            br_threshold: usize::MAX,
            mp_threshold: usize::MAX,
            mp_chunk_size: 1_048_576, // 1MiB
        }
    }

    // for download:
    //   Some - byte-range size
    //   None - for not using byte-range get
    pub fn get_download_chunk_size(&self, objsz: usize) -> Option<usize> {
        if self.max_concurrency > 1 && objsz > self.br_threshold {
            let range = self.br_threshold / self.max_concurrency;
            return Some(range);
        }
        None
    }

    // for upload:
    //   Some - chunk size
    //   None - for not using multipart upload
    pub fn get_upload_chunk_size(&self, objsz: usize) -> Option<usize> {
        if self.max_concurrency > 1 && objsz > self.mp_threshold {
            return Some(self.mp_chunk_size);
        }
        None
    }
}

pub struct S3TransferManager {
    client: S3Client,
    config: Option<S3TransferConfig>,
    set_progress_length: Option<Box<dyn Fn(usize)>>,
    progress_callback: Option<Box<dyn Fn(usize)>>,
    progress_finished: Option<Box<dyn Fn()>>,
}

impl S3TransferManager {
    pub async fn default() -> Self {
        let shared_config = aws_config::load_from_env().await;
        let client = S3Client::new(&shared_config);
        Self::new_with_bar(client)
    }

    pub fn new(client: S3Client) -> Self {
        Self {
            client: client,
            config: None,
            set_progress_length: None,
            progress_callback: None,
            progress_finished: None,
        }
    }

    pub fn new_with_bar(client: S3Client) -> Self {
        use indicatif::ProgressBar;
        use indicatif::ProgressStyle;

        let bar = ProgressBar::new(0);
        bar.set_style(ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {binary_bytes_per_sec} {pos:>7}/{len:7} {msg}")
            .unwrap()
            .progress_chars("##-"));
        let clone = bar.clone();
        let set = move |x| { clone.set_length(x as u64) };
        let clone = bar.clone();
        let update = move |x| { clone.inc(x as u64) };
        let finish = move | | { bar.finish() };
        Self::new(client).with_update_progress(set, update, finish)
    }

    pub fn with_config(mut self, config: S3TransferConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_update_progress(mut self,
            set_progress_length: impl Fn(usize) + 'static,
            progress_callback: impl Fn(usize) + 'static,
            progress_finished: impl Fn() + 'static
    ) -> Self {
        self.set_progress_length = Some(Box::new(set_progress_length));
        self.progress_callback = Some(Box::new(progress_callback));
        self.progress_finished = Some(Box::new(progress_finished));
        self
    }

    fn error_handler_get_object<R>(&self, sdk_err: aws_sdk_s3::error::SdkError<GetObjectError, R>) -> Result<(), Error>
    where
        R: std::fmt::Debug + Send + Sync + 'static,
    {
        match sdk_err {
            aws_sdk_s3::error::SdkError::ServiceError(err) => {
                match err.into_err() {
                    GetObjectError::NoSuchKey(e) => {
                        trace!(" - {}", e);
                        return Err(Error::new(ErrorKind::NotFound, e));
                    },
                    GetObjectError::InvalidObjectState(e) => {
                        trace!(" - {}", e);
                        return Err(Error::new(ErrorKind::Other, e));
                    },
                    e @ _ => {
                        trace!(" - {}", e);
                        return Err(Error::new(ErrorKind::Other, e));
                    },
                }
            },
            err @ _ => {
                trace!(" - {}", err);
                return Err(Error::new(ErrorKind::Other, err));
            },
        }
    }

    pub async fn download(&self, s3uri: &str, buf: &mut Vec<u8>) -> Result<(), Error> {
        let uri = S3Uri::parse(s3uri).expect("valid S3 Uri");
        match self.client.get_object()
                    .bucket(uri.bucket)
                    .key(uri.key)
                    .send()
                    .await
        {
            Ok(output) => {
                // set total length if we have set function
                if let Some(set) = &self.set_progress_length {
                    (set)(output.content_length.map(|x| x as usize).unwrap_or(0));
                }
                let stream = ByteStreamProgress::new(
                    output.body,
                    self.progress_callback
                        .as_ref()
                        .map(|cb| cb.as_ref())
                );
                let mut reader = stream.into_async_read();
                reader.read_to_end(buf).await?;
                if let Some(finish) = &self.progress_finished {
                    (finish)();
                }
                return Ok(());
            },
            Err(sdk_err) => {
                return self.error_handler_get_object(sdk_err);
            }
        }
    }

    pub async fn download_file(&self, s3uri: &str, path: impl AsRef<Path>) -> Result<(), Error> {
        let uri = S3Uri::parse(s3uri).expect("valid S3 Uri");
        match self.client.get_object()
                    .bucket(uri.bucket)
                    .key(uri.key)
                    .send()
                    .await
        {
            Ok(output) => {
                // set total length if we have set function
                if let Some(set_progress_length) = &self.set_progress_length {
                    (set_progress_length)(output.content_length.map(|x| x as usize).unwrap_or(0));
                }
                let stream = ByteStreamProgress::new(
                    output.body,
                    self.progress_callback
                        .as_ref()
                        .map(|cb| cb.as_ref())
                );
                let mut reader = stream.into_async_read();
                let file = tokio::fs::File::create(path).await?;
                let mut writer = tokio::io::BufWriter::new(file);
                tokio::io::copy_buf(&mut reader, &mut writer).await?;
                if let Some(finish) = &self.progress_finished {
                    (finish)();
                }
                return Ok(());
            },
            Err(sdk_err) => {
                return self.error_handler_get_object(sdk_err);
            }
        }
    }

    /*
    pub fn resumeDownloadFile() -> Result<()> {
        todo!();
    }
    */
}
