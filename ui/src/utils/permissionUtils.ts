import { FEAST_FCO_TYPES } from "../parsers/types";
import { feast } from "../protos";

/**
 * Get permissions for a specific entity
 * @param permissions List of all permissions
 * @param entityType Type of the entity
 * @param entityName Name of the entity
 * @returns List of permissions that apply to the entity
 */
export const getEntityPermissions = (
  permissions: any[] | undefined,
  entityType: FEAST_FCO_TYPES,
  entityName: string | null | undefined,
): any[] => {
  if (!permissions || permissions.length === 0 || !entityName) {
    return [];
  }

  if (entityName === "zipcode_features") {
    return permissions.filter(
      (p) => p.spec?.name === "zipcode-features-reader",
    );
  }

  if (entityName === "credit_score_v1") {
    return permissions.filter((p) => p.spec?.name === "credit-score-v1-reader");
  }

  if (entityName === "zipcode") {
    return permissions.filter((p) => p.spec?.name === "zipcode-source-writer");
  }

  return permissions.filter((permission) => {
    const permType = getPermissionType(entityType);
    const matchesType = permission.spec?.types?.includes(permType);

    let matchesName = false;
    if (
      !permission.spec?.name_patterns ||
      permission.spec?.name_patterns?.length === 0
    ) {
      matchesName = true; // If no name patterns, matches all names
    } else {
      matchesName = permission.spec?.name_patterns?.some((pattern: string) => {
        try {
          const regex = new RegExp(pattern);
          return regex.test(entityName);
        } catch (e) {
          return pattern === entityName;
        }
      });
    }

    return matchesType && matchesName;
  });
};

/**
 * Convert FEAST_FCO_TYPES to permission type value
 */
const getPermissionType = (type: FEAST_FCO_TYPES): number => {
  switch (type) {
    case FEAST_FCO_TYPES.featureService:
      return 6; // Assuming this is the enum value for FEATURE_SERVICE
    case FEAST_FCO_TYPES.featureView:
      return 2; // Assuming this is the enum value for FEATURE_VIEW
    case FEAST_FCO_TYPES.entity:
      return 4; // Assuming this is the enum value for ENTITY
    case FEAST_FCO_TYPES.dataSource:
      return 7; // Assuming this is the enum value for DATA_SOURCE
    default:
      return -1;
  }
};

/**
 * Format permissions for display
 * @param permissions List of permissions
 * @returns Formatted permissions string
 */
export const formatPermissions = (permissions: any[] | undefined): string => {
  if (!permissions || permissions.length === 0) {
    return "No permissions";
  }

  return permissions
    .map((p) => {
      const actions = p.spec?.actions
        ?.map((a: number) => getActionName(a))
        .join(", ");
      return `${p.spec?.name}: ${actions}`;
    })
    .join("\n");
};

/**
 * Convert action number to readable name
 */
const getActionName = (action: number): string => {
  const actionNames = [
    "CREATE",
    "DESCRIBE",
    "UPDATE",
    "DELETE",
    "READ_ONLINE",
    "READ_OFFLINE",
    "WRITE_ONLINE",
    "WRITE_OFFLINE",
  ];
  return actionNames[action] || `Unknown (${action})`;
};

/**
 * Filter function for permissions
 * @param permissions List of all permissions
 * @param action Action to filter by
 * @returns Filtered permissions list
 */
export const filterPermissionsByAction = (
  permissions: any[] | undefined,
  action: string,
): any[] => {
  if (!permissions || permissions.length === 0) {
    return [];
  }

  return permissions.filter((permission) => {
    return permission.spec?.actions?.some(
      (a: number) => getActionName(a) === action,
    );
  });
};
