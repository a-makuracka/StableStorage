use std::path::PathBuf;
use sha2::{Digest, Sha256};
use std::{path::{Path}};
use tokio::fs as async_fs;
use tokio::io::{AsyncWriteExt};
use base64::engine::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;



// Define a structure representing the stable storage
pub struct FileSystemStableStorage {
    root_storage_dir: PathBuf,
}


#[async_trait::async_trait]
pub trait StableStorage: Send + Sync {
    /// Stores `value` under `key`.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;

    /// Retrieves value stored under `key`.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn get(&self, key: &str) -> Option<Vec<u8>>;

    /// Removes `key` and the value stored under it.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn remove(&mut self, key: &str) -> bool;
}


#[async_trait::async_trait]
impl StableStorage for FileSystemStableStorage {
    
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        // Check if the length of the key or value exceeds the specified limits.
        if key.len() > 255 || value.len() > 65535 {
            return Err("Invalid key or value length".to_string());
        }
    
        // Find hash for the given key.
        let key_hash = hash_key(key);
    
        // Create paths for temporary file and data file (based on the key hash).
        let temp_file_path = format!("{}/'{}'.tmp", self.root_storage_dir.display(), key_hash);
        let data_file_path = format!("{}/'{}'.data", self.root_storage_dir.display(), key_hash);
        
        // Create a temporary file using its path.
        let mut temp_file = async_fs::File::create(&temp_file_path)
            .await
            .map_err(|err| format!( "Error creating temp file in directory: {}",err))?;
            
        // Write the value to the temporary file.
        temp_file
            .write_all(value)
            .await
            .map_err(|err| format!("Error writing to temp file: {}", err))?;
    
        // Sync the temporary file (to ensure data transfer to disk).
        temp_file
            .sync_data()
            .await
            .map_err(|err| format!("Error syncing temp file: {}", err))?;
    
        // Rename the temporary file to the data file.
        async_fs::rename(&temp_file_path, &data_file_path)
            .await
            .map_err(|err| format!("Error renaming temp file: {}", err))?;
    
        // Sync the directory (to ensure data transfer to disk).
        let root_dir = async_fs::File::open(&self.root_storage_dir)
            .await
            .map_err(|err| format!("Error opening root storage directory: {}", err))?;
        root_dir
            .sync_data()
            .await
            .map_err(|err| format!("Error syncing directory: {}", err))?;

        print!("");
    
        Ok(())
    }
    
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        // Find hash for the given key.
        let key_hash = hash_key(key);
    
        // Create the path to the data file associated with the key.
        let data_file_path_str = format!("{}/'{}'.data", self.root_storage_dir.display(), key_hash);

        let data_file_path = Path::new(&data_file_path_str);

        // Check if the file exists.
        if !data_file_path.exists() {
            return None; 
        }
    
        // Read the contents of the file.
        match async_fs::read(data_file_path_str).await {
            Ok(contents) => Some(contents), // Return the file contents if read successful.
            Err(err) => {
                eprintln!("Error reading file: {}", err); // Print an error message.
                None // Return None in case of an error.
            }
        }
    }
    
    async fn remove(&mut self, key: &str) -> bool {
        // Find hash for the given key.
        let key_hash = hash_key(key);
    
        // Create the path to the data file associated with the key.
        let data_file_path_str = format!("{}/'{}'.data", self.root_storage_dir.display(), key_hash);

        let data_file_path = Path::new(&data_file_path_str);

        // Check if the file exists.
        if !data_file_path.exists() {
            return false; 
        }
    
        match async_fs::remove_file(&data_file_path_str).await {
            Ok(_) => {
                let root_dir_res = async_fs::File::open(&self.root_storage_dir).await;
                if let Ok(root_dir) = root_dir_res {
                    // Sync the directory.
                    if let Err(err) = root_dir.sync_data().await {
                        eprintln!("Error syncing directory: {}", err);
                    }
                } else {
                    eprintln!("Error opening directory.");
                }
        
                true // Return true if file removal is successful.
            }
            Err(err) => {
                eprintln!("Error removing file: {}", err); // Print an error message.
                false // Return false if an error occurs during file removal.
            }
        }
    }
}


fn into_valid_file_name(input: &str) -> String {
    input.replace("/", "")
}


fn hash_key(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key);
    let hash_result = hasher.finalize();

    // Encode the hash result in base64
    let encoded_hash = BASE64.encode(&hash_result);
    
    // Return encoded_hash as a valid file name. 
    into_valid_file_name(&encoded_hash)
}


/// Creates a new instance of stable storage.
pub async fn build_stable_storage(root_storage_dir: PathBuf) -> Box<dyn StableStorage> {
    Box::new(FileSystemStableStorage { root_storage_dir })
}
