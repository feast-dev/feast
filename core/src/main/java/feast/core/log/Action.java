package feast.core.log;

/** Actions taken for audit logging purposes */
public enum Action {
  // Job-related actions
  SUBMIT,
  STATUS_CHANGE,
  ABORT,

  // Spec-related
  UPDATE,
  REGISTER,

  // Storage-related
  ADD,
  SCHEMA_UPDATE,
}
