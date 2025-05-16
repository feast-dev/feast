/**
 * Generates CLI commands for creating Feast objects
 * This module provides utilities for generating CLI commands that users can run
 * to create Feast objects in their local registry
 */

/**
 * Simulates writing an object to the local Feast registry
 * This is a mock implementation since browser environments can't directly access the filesystem
 * Instead, we generate CLI commands that users can run to create objects
 * 
 * @param objectType The type of Feast object (entity, data_source, feature_view, feature_service, permission)
 * @param objectData The data for the object to be created
 * @param registryPath Optional path to the registry file. If not provided, will use the default path
 * @returns A promise that resolves with a success message
 */
export const writeToLocalRegistry = async (
  objectType: string,
  objectData: any,
  registryPath?: string
): Promise<{ success: boolean; message: string }> => {
  try {
    const objectName = objectData.name || 'object';
    
    return {
      success: true,
      message: `To create this ${objectType} in your local Feast registry, use the CLI command shown below.`
    };
  } catch (error) {
    console.error('Error generating CLI command:', error);
    return {
      success: false,
      message: error instanceof Error ? error.message : 'Unknown error'
    };
  }
};

/**
 * Generates a CLI command for creating a Feast object
 * @param objectType The type of Feast object (entity, data_source, feature_view, feature_service, permission)
 * @param objectData The data for the object to be created
 * @returns A string containing the CLI command
 */
export const generateCliCommand = (
  objectType: string,
  objectData: any
): string => {
  const fileName = `${objectType}_${objectData.name?.toLowerCase().replace(/\s+/g, '_') || 'example'}.py`;
  
  let codeContent = '';
  
  switch (objectType) {
    case 'entity':
      codeContent = `
from feast import Entity
from feast.value_type import ValueType

entity = Entity(
    name="${objectData.name}",
    value_type=ValueType.${objectData.valueType},
    join_key="${objectData.joinKey || objectData.name}",
    description="${objectData.description || ''}",
    tags=${JSON.stringify(objectData.tags || {})},
    owner="${objectData.owner || ''}"
)

# Then apply it using the Feast CLI:
# feast apply ${fileName}`;
      break;
      
    case 'data_source':
      codeContent = `
from feast import FileSource

source = FileSource(
    name="${objectData.name}",
    path="${objectData.path || ''}",
    timestamp_field="${objectData.timestampField || ''}",
    description="${objectData.description || ''}",
    tags=${JSON.stringify(objectData.tags || {})}
)

# Then apply it using the Feast CLI:
# feast apply ${fileName}`;
      break;
      
    case 'feature_view':
      const featuresCode = (objectData.features || []).map((f: any) => 
        `    Feature(name="${f.name}", dtype=${f.valueType})`
      ).join(",\n");
      
      const entitiesCode = (objectData.entities || []).map((e: any) => 
        `"${e}"`
      ).join(", ");
      
      codeContent = `
from feast import FeatureView, Feature
from feast.value_type import ValueType
from datetime import timedelta

# You'll need to get references to your entities and data source
# This is a simplified example - you may need to adjust based on your actual setup
data_source = feast.get_data_source("${objectData.dataSource || ''}")
entities = [feast.get_entity(e) for e in [${entitiesCode}]]

feature_view = FeatureView(
    name="${objectData.name}",
    entities=entities,
    ttl=timedelta(seconds=${objectData.ttlSeconds || 86400}),
    features=[
${featuresCode}
    ],
    online=True,
    source=data_source,
    tags=${JSON.stringify(objectData.tags || {})},
    owner="${objectData.owner || ''}",
    description="${objectData.description || ''}"
)

# Then apply it using the Feast CLI:
# feast apply ${fileName}`;
      break;
      
    case 'feature_service':
      const featureReferenceCode = Object.entries(objectData.featureReferences || {}).map(([featureView, features]: [string, any]) => {
        const featuresStr = (features as string[]).map(f => `"${f}"`).join(", ");
        return `    FeatureReference(name="${featureView}", features=[${featuresStr}])`;
      }).join(",\n");
      
      codeContent = `
from feast import FeatureService, FeatureReference

feature_service = FeatureService(
    name="${objectData.name}",
    features=[
${featureReferenceCode}
    ],
    tags=${JSON.stringify(objectData.tags || {})},
    owner="${objectData.owner || ''}",
    description="${objectData.description || ''}"
)

# Then apply it using the Feast CLI:
# feast apply ${fileName}`;
      break;
      
    case 'permission':
      codeContent = `
from feast import Permission
from feast.permissions.action import Action

permission = Permission(
    name="${objectData.name}",
    principal="${objectData.principal || ''}",
    resource="${objectData.resource || ''}",
    action=Action.${objectData.action || 'READ'},
    description="${objectData.description || ''}",
    tags=${JSON.stringify(objectData.tags || {})},
    owner="${objectData.owner || ''}"
)

# Then apply it using the Feast CLI:
# feast apply ${fileName}`;
      break;
      
    default:
      return `# Unknown object type: ${objectType}`;
  }
  
  return `# Create a Python file named ${fileName} with the following content:\n${codeContent}`;
};
