use crate::config::MemoryConfig;

#[derive(Debug, Clone)]
pub struct AclContext {
    pub device: String,
    pub is_admin: bool,
    /// Config-driven list of registered device names (from `acl.device_categories`).
    pub device_categories: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AclDecision {
    Allowed(String), // reason
    Denied(String),  // reason
}

const BASE_CATEGORIES: &[&str] = &[
    "base",
    "infrastructure",
    "projects",
    "workflow_global",
    "nodes",
    "ecosystem",
];

const RESERVED_CATEGORIES: &[&str] = &["meta", "usage_stats"];

pub fn get_context(config: &MemoryConfig) -> AclContext {
    let device = config.acl.device_name.clone();
    let is_admin = config.acl.admin_devices.iter().any(|d| d == &device);
    AclContext {
        device,
        is_admin,
        device_categories: config.acl.device_categories.clone(),
    }
}

pub fn authorize_write(category: &str, ctx: &AclContext) -> AclDecision {
    // Reserved categories — no direct write
    if RESERVED_CATEGORIES.contains(&category) {
        return AclDecision::Denied("reserved_category".into());
    }

    // Admin can do everything else
    if ctx.is_admin {
        return AclDecision::Allowed("admin".into());
    }

    // Base categories — admin only
    if BASE_CATEGORIES.contains(&category) {
        return AclDecision::Denied("base_requires_admin".into());
    }

    // Device categories — only the device itself
    if ctx.device_categories.iter().any(|d| d == category) {
        if category == ctx.device {
            return AclDecision::Allowed("device_self".into());
        }
        return AclDecision::Denied("device_requires_self".into());
    }

    // Workflow categories
    if let Some(suffix) = category.strip_prefix("workflow_") {
        if suffix == "global" {
            return AclDecision::Denied("workflow_global_requires_admin".into());
        }
        if ctx.device_categories.iter().any(|d| d == suffix) {
            if suffix == ctx.device {
                return AclDecision::Allowed("workflow_device_self".into());
            }
            return AclDecision::Denied("workflow_device_requires_self".into());
        }
        return AclDecision::Denied("workflow_project_requires_admin".into());
    }

    // Agent-scoped categories: <X>-agent device can write <X>_* categories.
    // E.g. device "notes-agent" -> categories with prefix "notes_".
    if let Some(agent_prefix) = ctx.device.strip_suffix("-agent") {
        let expected_prefix = format!("{}_", agent_prefix);
        if category.starts_with(&expected_prefix) {
            return AclDecision::Allowed("agent_scoped".into());
        }
    }

    // Default: deny for non-admin
    AclDecision::Denied("not_authorized".into())
}

pub fn authorize_read(category: &str, _ctx: &AclContext) -> AclDecision {
    // All categories readable by all authenticated devices
    let _ = category;
    AclDecision::Allowed("read_all".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin_ctx() -> AclContext {
        AclContext {
            device: "server-a".to_string(),
            is_admin: true,
            device_categories: vec![
                "server-a".to_string(),
                "device-b".to_string(),
                "tablet-c".to_string(),
            ],
        }
    }

    fn device_ctx(device: &str) -> AclContext {
        AclContext {
            device: device.to_string(),
            is_admin: false,
            device_categories: vec![
                "server-a".to_string(),
                "device-b".to_string(),
                "tablet-c".to_string(),
            ],
        }
    }

    #[test]
    fn test_reserved_categories_deny_all() {
        let ctx = admin_ctx();
        for cat in &["meta", "usage_stats"] {
            let result = authorize_write(cat, &ctx);
            assert!(
                matches!(result, AclDecision::Denied(_)),
                "reserved {} should be denied even for admin",
                cat
            );
        }
    }

    #[test]
    fn test_base_categories_deny_non_admin() {
        let ctx = device_ctx("device-b");
        for cat in &[
            "base",
            "projects",
            "infrastructure",
            "workflow_global",
            "nodes",
        ] {
            let result = authorize_write(cat, &ctx);
            assert!(
                matches!(result, AclDecision::Denied(_)),
                "base {} should deny non-admin",
                cat
            );
        }
    }

    #[test]
    fn test_admin_allowed_all_except_reserved() {
        let ctx = admin_ctx();
        // Admin can write base
        assert!(matches!(
            authorize_write("base", &ctx),
            AclDecision::Allowed(_)
        ));
        assert!(matches!(
            authorize_write("projects", &ctx),
            AclDecision::Allowed(_)
        ));
        assert!(matches!(
            authorize_write("device-b", &ctx),
            AclDecision::Allowed(_)
        ));
    }

    #[test]
    fn test_device_self_allowed() {
        let ctx = device_ctx("device-b");
        assert!(matches!(
            authorize_write("device-b", &ctx),
            AclDecision::Allowed(_)
        ));
    }

    #[test]
    fn test_device_other_denied() {
        let ctx = device_ctx("device-b");
        assert!(matches!(
            authorize_write("server-a", &ctx),
            AclDecision::Denied(_)
        ));
        assert!(matches!(
            authorize_write("tablet-c", &ctx),
            AclDecision::Denied(_)
        ));
    }

    #[test]
    fn test_workflow_device_self_allowed() {
        let ctx = device_ctx("device-b");
        assert!(matches!(
            authorize_write("workflow_device-b", &ctx),
            AclDecision::Allowed(_)
        ));
    }

    #[test]
    fn test_workflow_global_denied_non_admin() {
        let ctx = device_ctx("device-b");
        assert!(matches!(
            authorize_write("workflow_global", &ctx),
            AclDecision::Denied(_)
        ));
    }

    #[test]
    fn test_workflow_other_device_denied() {
        let ctx = device_ctx("device-b");
        assert!(matches!(
            authorize_write("workflow_server-a", &ctx),
            AclDecision::Denied(_)
        ));
    }

    #[test]
    fn test_read_all_allowed() {
        let ctx = device_ctx("device-b");
        assert!(matches!(
            authorize_read("base", &ctx),
            AclDecision::Allowed(_)
        ));
        assert!(matches!(
            authorize_read("meta", &ctx),
            AclDecision::Allowed(_)
        ));
        assert!(matches!(
            authorize_read("anything", &ctx),
            AclDecision::Allowed(_)
        ));
    }

    #[test]
    fn test_unknown_category_denied_non_admin() {
        let ctx = device_ctx("device-b");
        assert!(matches!(
            authorize_write("custom_project", &ctx),
            AclDecision::Denied(_)
        ));
    }

    #[test]
    fn test_agent_scope_self_allowed() {
        let ctx = device_ctx("notes-agent");
        for cat in &[
            "notes_sessions",
            "notes_inbox",
            "notes_archive",
            "notes_drafts",
            "notes_journal",
            "notes_tags",
        ] {
            assert!(
                matches!(authorize_write(cat, &ctx), AclDecision::Allowed(_)),
                "agent scope: notes-agent should write {}",
                cat
            );
        }
    }

    #[test]
    fn test_agent_scope_other_prefix_denied() {
        let ctx = device_ctx("notes-agent");
        assert!(matches!(
            authorize_write("other_sessions", &ctx),
            AclDecision::Denied(_)
        ));
    }

    #[test]
    fn test_agent_scope_no_underscore_denied() {
        // category "notes" (without underscore) does not match agent scope "notes_"
        let ctx = device_ctx("notes-agent");
        assert!(matches!(
            authorize_write("notes", &ctx),
            AclDecision::Denied(_)
        ));
    }

    #[test]
    fn test_agent_scope_admin_still_overrides() {
        // even with -agent suffix, admin keeps full access
        let ctx = AclContext {
            device: "notes-agent".into(),
            is_admin: true,
            device_categories: vec![],
        };
        assert!(matches!(
            authorize_write("base", &ctx),
            AclDecision::Allowed(_)
        ));
    }

    #[test]
    fn test_device_not_in_device_categories_denied() {
        // A device not listed in device_categories cannot write its own name category
        let ctx = AclContext {
            device: "unknown-device".into(),
            is_admin: false,
            device_categories: vec!["server-a".to_string(), "device-b".to_string()],
        };
        assert!(matches!(
            authorize_write("unknown-device", &ctx),
            AclDecision::Denied(_)
        ));
    }

    #[test]
    fn test_empty_device_categories_no_device_writes() {
        // With empty device_categories, no device categories exist to write
        let ctx = AclContext {
            device: "device-b".into(),
            is_admin: false,
            device_categories: vec![],
        };
        assert!(matches!(
            authorize_write("device-b", &ctx),
            AclDecision::Denied(_)
        ));
    }
}
