import { useMemo } from "react";
import Fuse from "fuse.js";
import useLoadRegistry from "../queries/useLoadRegistry";
import { useContext } from "react";
import RegistryPathContext from "../contexts/RegistryPathContext";
import { FEAST_FV_TYPES } from "../parsers/mergedFVTypes";

export interface SearchResult {
  id: string;
  type: string;
  name: string;
  description?: string;
  tags?: Record<string, string>;
  url: string;
}

const useGlobalSearch = (searchTerm: string) => {
  const registryUrl = useContext(RegistryPathContext);
  const registryQuery = useLoadRegistry(registryUrl);
  
  const searchResults = useMemo(() => {
    if (!registryQuery.data || !searchTerm || searchTerm.length < 2) {
      return [];
    }
    
    const { objects, mergedFVList } = registryQuery.data;
    const allSearchableItems: SearchResult[] = [];
    
    // Add Feature Views (regular, on-demand, stream)
    mergedFVList.forEach((fv) => {
      let description = "";
      let tags = {};
      
      if (fv.type === FEAST_FV_TYPES.regular) {
        description = fv.object.spec?.description || "";
        tags = fv.object.spec?.tags || {};
      } else if (fv.type === FEAST_FV_TYPES.ondemand) {
        description = fv.object.spec?.description || "";
        tags = fv.object.spec?.tags || {};
      } else if (fv.type === FEAST_FV_TYPES.stream) {
        description = fv.object.spec?.description || "";
        tags = fv.object.spec?.tags || {};
      }
      
      allSearchableItems.push({
        id: `${fv.type}-${fv.name}`,
        type: fv.type === FEAST_FV_TYPES.regular 
          ? "Feature View" 
          : fv.type === FEAST_FV_TYPES.ondemand 
            ? "On-Demand Feature View" 
            : "Stream Feature View",
        name: fv.name,
        description,
        tags,
        url: `/p/${registryQuery.data.project}/feature-view/${fv.name}`
      });
    });
    
    // Add Entities
    objects.entities?.forEach((entity) => {
      allSearchableItems.push({
        id: `entity-${entity.spec?.name}`,
        type: "Entity",
        name: entity.spec?.name || "",
        description: entity.spec?.description || "",
        tags: entity.spec?.tags || {},
        url: `/p/${registryQuery.data.project}/entity/${entity.spec?.name}`
      });
    });
    
    // Add Data Sources
    objects.dataSources?.forEach((ds) => {
      allSearchableItems.push({
        id: `dataSource-${ds.name}`,
        type: "Data Source",
        name: ds.name || "",
        description: ds.description || "",
        tags: {},
        url: `/p/${registryQuery.data.project}/data-source/${ds.name}`
      });
    });
    
    // Add Feature Services
    objects.featureServices?.forEach((fs) => {
      allSearchableItems.push({
        id: `featureService-${fs.spec?.name}`,
        type: "Feature Service",
        name: fs.spec?.name || "",
        description: fs.spec?.description || "",
        tags: fs.spec?.tags || {},
        url: `/p/${registryQuery.data.project}/feature-service/${fs.spec?.name}`
      });
    });
    
    // Add Saved Datasets
    objects.savedDatasets?.forEach((ds) => {
      allSearchableItems.push({
        id: `dataset-${ds.spec?.name}`,
        type: "Dataset",
        name: ds.spec?.name || "",
        description: "", // SavedDataset doesn't have a description field
        tags: {},
        url: `/p/${registryQuery.data.project}/data-set/${ds.spec?.name}`
      });
    });
    
    // Configure Fuse.js for fuzzy searching
    const fuseOptions = {
      includeScore: true,
      threshold: 0.4,
      keys: [
        { name: 'name', weight: 2 },
        { name: 'description', weight: 1 },
        { name: 'type', weight: 1 },
        { name: 'tags', weight: 1 }
      ]
    };
    
    const fuse = new Fuse(allSearchableItems, fuseOptions);
    const results = fuse.search(searchTerm);
    
    // Return top 10 results
    return results.slice(0, 10).map(result => result.item);
  }, [registryQuery.data, searchTerm]);
  
  return {
    ...registryQuery,
    searchResults
  };
};

export default useGlobalSearch;
