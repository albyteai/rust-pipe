#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error(
        "Invalid worker ID '{value}': must be alphanumeric with dots, hyphens, or underscores"
    )]
    InvalidWorkerId { value: String },
    #[error("Invalid Docker image name '{value}'")]
    InvalidImageName { value: String },
    #[error("Invalid hostname '{value}'")]
    InvalidHostname { value: String },
    #[error("Invalid username '{value}'")]
    InvalidUsername { value: String },
    #[error("Dangerous value rejected '{value}': {reason}")]
    DangerousValue { value: String, reason: String },
}

fn is_safe_identifier_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_'
}

pub fn validate_worker_id(id: &str) -> Result<(), ValidationError> {
    if id.is_empty()
        || id.len() > 128
        || !id.starts_with(|c: char| c.is_ascii_alphanumeric())
        || !id.chars().all(is_safe_identifier_char)
    {
        return Err(ValidationError::InvalidWorkerId {
            value: id.to_string(),
        });
    }
    Ok(())
}

pub fn validate_docker_image(image: &str) -> Result<(), ValidationError> {
    if image.is_empty() || image.len() > 256 {
        return Err(ValidationError::InvalidImageName {
            value: image.to_string(),
        });
    }
    let valid = image.chars().all(|c| {
        c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' || c == '/' || c == ':'
    });
    if !valid || !image.starts_with(|c: char| c.is_ascii_alphanumeric()) || image.contains("..") {
        return Err(ValidationError::InvalidImageName {
            value: image.to_string(),
        });
    }
    Ok(())
}

pub fn validate_hostname(host: &str) -> Result<(), ValidationError> {
    if host.is_empty()
        || host.len() > 253
        || !host.starts_with(|c: char| c.is_ascii_alphanumeric())
        || !host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_')
    {
        return Err(ValidationError::InvalidHostname {
            value: host.to_string(),
        });
    }
    Ok(())
}

pub fn validate_username(user: &str) -> Result<(), ValidationError> {
    if user.is_empty()
        || user.len() > 64
        || !user.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        || !user
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Err(ValidationError::InvalidUsername {
            value: user.to_string(),
        });
    }
    Ok(())
}

pub fn validate_no_shell_metacharacters(
    value: &str,
    field_name: &str,
) -> Result<(), ValidationError> {
    const DANGEROUS: &[char] = &[
        '`', '$', '(', ')', '{', '}', ';', '|', '&', '<', '>', '\n', '\r', '\0',
    ];
    if value.chars().any(|c| DANGEROUS.contains(&c)) {
        return Err(ValidationError::DangerousValue {
            value: value.to_string(),
            reason: format!("'{}' contains shell metacharacters", field_name),
        });
    }
    Ok(())
}

pub fn validate_file_path(path: &str, field_name: &str) -> Result<(), ValidationError> {
    validate_no_shell_metacharacters(path, field_name)?;
    if path.contains("..") {
        return Err(ValidationError::DangerousValue {
            value: path.to_string(),
            reason: format!("'{}' contains path traversal", field_name),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_worker_ids() {
        assert!(validate_worker_id("worker-1").is_ok());
        assert!(validate_worker_id("my.worker_v2").is_ok());
        assert!(validate_worker_id("abc123").is_ok());
    }

    #[test]
    fn test_invalid_worker_ids() {
        assert!(validate_worker_id("").is_err());
        assert!(validate_worker_id("-bad").is_err());
        assert!(validate_worker_id("has space").is_err());
        assert!(validate_worker_id("has;semicolon").is_err());
        assert!(validate_worker_id("$(inject)").is_err());
    }

    #[test]
    fn test_valid_docker_images() {
        assert!(validate_docker_image("nginx:latest").is_ok());
        assert!(validate_docker_image("registry.io/org/image:v1.2.3").is_ok());
        assert!(validate_docker_image("ubuntu").is_ok());
    }

    #[test]
    fn test_invalid_docker_images() {
        assert!(validate_docker_image("").is_err());
        assert!(validate_docker_image("--privileged").is_err());
        assert!(validate_docker_image("img;rm -rf /").is_err());
        assert!(validate_docker_image("../escape").is_err());
    }

    #[test]
    fn test_valid_hostnames() {
        assert!(validate_hostname("example.com").is_ok());
        assert!(validate_hostname("10.0.0.1").is_ok());
        assert!(validate_hostname("my-host.internal").is_ok());
    }

    #[test]
    fn test_invalid_hostnames() {
        assert!(validate_hostname("").is_err());
        assert!(validate_hostname("-bad.com").is_err());
        assert!(validate_hostname("host;evil").is_err());
    }

    #[test]
    fn test_valid_usernames() {
        assert!(validate_username("root").is_ok());
        assert!(validate_username("deploy_user").is_ok());
        assert!(validate_username("_service").is_ok());
    }

    #[test]
    fn test_invalid_usernames() {
        assert!(validate_username("").is_err());
        assert!(validate_username("-bad").is_err());
        assert!(validate_username("user;evil").is_err());
        assert!(validate_username("$(whoami)").is_err());
    }

    #[test]
    fn test_shell_metacharacter_rejection() {
        assert!(validate_no_shell_metacharacters("safe-value_123", "test").is_ok());
        assert!(validate_no_shell_metacharacters("$(inject)", "test").is_err());
        assert!(validate_no_shell_metacharacters("a;b", "test").is_err());
        assert!(validate_no_shell_metacharacters("a|b", "test").is_err());
        assert!(validate_no_shell_metacharacters("a`b`", "test").is_err());
    }

    #[test]
    fn test_path_traversal_rejection() {
        assert!(validate_file_path("/home/user/.ssh/id_rsa", "key").is_ok());
        assert!(validate_file_path("../../etc/passwd", "key").is_err());
        assert!(validate_file_path("/path/$(cmd)/file", "key").is_err());
    }
}
