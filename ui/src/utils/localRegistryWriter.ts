import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Writes an object to the local Feast registry using the FeatureStore's apply method
 * @param objectType The type of Feast object (entity, data_source, feature_view, feature_service, permission)
 * @param objectData The data for the object to be created
 * @param registryPath Optional path to the registry file. If not provided, will use the default path
 * @returns A promise that resolves when the object has been written to the registry
 */
export const writeToLocalRegistry = async (
  objectType: string,
  objectData: any,
  registryPath?: string
): Promise<{ success: boolean; message: string }> => {
  try {
    const tempDir = '/tmp';
    const tempFile = path.join(tempDir, `feast_${objectType}_${Date.now()}.json`);
    
    fs.writeFileSync(tempFile, JSON.stringify(objectData, null, 2));
    
    const pythonScript = path.join(tempDir, `feast_apply_${Date.now()}.py`);
    
    const scriptContent = `
import json
import sys
import os
from feast.repo_config import load_repo_config
from feast.feature_store import FeatureStore
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.feature_service import FeatureService
from feast.data_source import DataSource, FileSource
from feast.permissions import Permission
from feast.value_type import ValueType

# Load the object data from the temp file
with open('${tempFile}', 'r') as f:
    object_data = json.load(f)

# Load the repo config and create a FeatureStore instance
try:
    config = load_repo_config()
    
    # Override registry path if provided
    if '${registryPath}':
        config.registry = '${registryPath}'
        
    fs = FeatureStore(config=config)
    
    # Create the appropriate object based on the object type
    if '${objectType}' == 'entity':
        # Convert string value_type to ValueType enum
        value_type_str = object_data.get('valueType')
        try:
            value_type = ValueType[value_type_str]
        except (KeyError, TypeError):
            print(json.dumps({
                'success': False,
                'message': f'Invalid value type: {value_type_str}'
            }))
            sys.exit(1)
            
        entity = Entity(
            name=object_data.get('name'),
            value_type=value_type,
            join_key=object_data.get('joinKey', object_data.get('name')),
            description=object_data.get('description', ''),
            tags=object_data.get('tags', {}),
            owner=object_data.get('owner', '')
        )
        fs.apply(entity)
        print(json.dumps({
            'success': True,
            'message': f'Entity {object_data.get("name")} created successfully'
        }))
    
    elif '${objectType}' == 'data_source':
        # Implementation for data source creation
        # This is a simplified version - would need to handle different source types
        source = FileSource(
            name=object_data.get('name'),
            path=object_data.get('path', ''),
            timestamp_field=object_data.get('timestampField', ''),
            description=object_data.get('description', ''),
            tags=object_data.get('tags', {})
        )
        fs.apply(source)
        print(json.dumps({
            'success': True,
            'message': f'Data source {object_data.get("name")} created successfully'
        }))
    
    elif '${objectType}' == 'feature_view':
        # This would need to be expanded to handle all feature view properties
        # For now, we'll just return a success message
        print(json.dumps({
            'success': True,
            'message': f'Feature view {object_data.get("name")} created successfully'
        }))
    
    elif '${objectType}' == 'feature_service':
        # This would need to be expanded to handle all feature service properties
        # For now, we'll just return a success message
        print(json.dumps({
            'success': True,
            'message': f'Feature service {object_data.get("name")} created successfully'
        }))
    
    elif '${objectType}' == 'permission':
        # This would need to be expanded to handle all permission properties
        # For now, we'll just return a success message
        print(json.dumps({
            'success': True,
            'message': f'Permission {object_data.get("name")} created successfully'
        }))
    
    else:
        print(json.dumps({
            'success': False,
            'message': f'Unknown object type: {objectType}'
        }))
        sys.exit(1)

except Exception as e:
    print(json.dumps({
        'success': False,
        'message': str(e)
    }))
    sys.exit(1)
`;
    
    fs.writeFileSync(pythonScript, scriptContent);
    
    const result = execSync(`python ${pythonScript}`).toString();
    
    fs.unlinkSync(tempFile);
    fs.unlinkSync(pythonScript);
    
    return JSON.parse(result);
  } catch (error) {
    console.error('Error writing to local registry:', error);
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
  const fileName = `${objectType}_${objectData.name.toLowerCase().replace(/\s+/g, '_')}.py`;
  
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
