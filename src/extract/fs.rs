use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::sync::Arc;
use tracing::{debug, error, trace};

/// Reads a `.json` file from `path`.
pub async fn read_json<T: serde::de::DeserializeOwned>(path: &str) -> anyhow::Result<T> {
    trace!(filepath=%path, "reading file");
    let file = tokio::fs::read(path).await?;

    trace!(filepath=%path, "file read - deserializing bytes");
    let data: T = serde_json::from_slice(&file)?;
    Ok(data)
}

/// Unzip a `.zip` file, `zip_file`, to a target directory, `dir`.
///
/// `std::fs::create_dir_all(to_dir)?` is used in creating `to_dir` path,
/// so directories will be created, as necessary, by the unzip() function.
pub async fn unzip(zip_file: &str, dir: &str) -> anyhow::Result<()> {
    debug!("unzipping {zip_file} to {dir}");

    // Open the file, but `std::fs` has to be used, instead of tokio.
    let file = std::fs::File::open(zip_file)?;
    let archive = zip::ZipArchive::new(file).map_err(|e| {
        error!("failed to open zip file at {}: {}", zip_file, e);
        e
    })?;
    let zip_length = archive.len();

    // Async wrappings for archive.
    let archive = Arc::new(std::sync::Mutex::new(archive));

    // Ensure the target directory exists.
    tokio::fs::create_dir_all(dir).await?;

    // Parallel iteration across zipped files.
    (0..zip_length).into_par_iter().for_each(|i| {
        let archive = archive.clone();
        let mut archive = archive.lock().expect("unlock zip archive");
        let mut file = archive.by_index(i).expect("file from zip archive");
        let outpath = format!("{dir}/{}", file.mangled_name().display());
        let outdir = std::path::Path::new(&outpath)
            .parent()
            .expect("parent directory of output path");

        // If output directory does not exist, create it.
        if !outdir.exists() {
            std::fs::create_dir_all(&outdir).expect("failed to create directory");
        }

        // Extract the file.
        let mut outfile = std::fs::File::create(&outpath).expect("creation of output file");
        trace!("extracting {} to {}", file.name(), outpath);
        std::io::copy(&mut file, &mut outfile).expect("copying of zip file to output");
    });

    debug!("{zip_file} unzipped to {dir}");

    Ok(())
}
