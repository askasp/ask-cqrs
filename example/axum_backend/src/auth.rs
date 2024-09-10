pub enum Role {
    User,
    Admin,
}

pub struct AuthUser {
    pub user_id: String,
    pub role: Role,
}
