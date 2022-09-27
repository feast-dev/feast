import {
  FeastFeatureColumnType,
  FeastFeatureViewType,
} from "./feastFeatureViews";
import { FeastODFVType } from "./feastODFVS";
import { FeastSFVType } from "./feastSFVS";
import { FeastRegistryType } from "./feastRegistry";

enum FEAST_FV_TYPES {
  regular = "regular",
  ondemand = "ondemand",
  stream = "stream"
}

interface regularFVInterface {
  name: string;
  type: FEAST_FV_TYPES.regular;
  features: FeastFeatureColumnType[];
  object: FeastFeatureViewType;
}

interface ODFVInterface {
  name: string;
  type: FEAST_FV_TYPES.ondemand;
  features: FeastFeatureColumnType[];
  object: FeastODFVType;
}

interface SFVInterface {
  name: string;
  type: FEAST_FV_TYPES.stream;
  features: FeastFeatureColumnType[];
  object: FeastSFVType;
}

type genericFVType = regularFVInterface | ODFVInterface | SFVInterface;

const mergedFVTypes = (objects: FeastRegistryType) => {
  const mergedFVMap: Record<string, genericFVType> = {};

  const mergedFVList: genericFVType[] = [];

  objects.featureViews?.forEach((fv) => {
    const obj: genericFVType = {
      name: fv.spec.name,
      type: FEAST_FV_TYPES.regular,
      features: fv.spec.features,
      object: fv,
    };

    mergedFVMap[fv.spec.name] = obj;
    mergedFVList.push(obj);
  });

  objects.onDemandFeatureViews?.forEach((odfv) => {
    const obj: genericFVType = {
      name: odfv.spec.name,
      type: FEAST_FV_TYPES.ondemand,
      features: odfv.spec.features,
      object: odfv,
    };

    mergedFVMap[odfv.spec.name] = obj;
    mergedFVList.push(obj);
  });

  objects.streamFeatureViews?.forEach((sfv) => {
    const obj: genericFVType = {
      name: sfv.spec.name,
      type: FEAST_FV_TYPES.stream,
      features: sfv.spec.features,
      object: sfv,
    };

    mergedFVMap[sfv.spec.name] = obj;
    mergedFVList.push(obj);
  });

  return { mergedFVMap, mergedFVList };
};

export default mergedFVTypes;
export { FEAST_FV_TYPES };
export type { genericFVType, regularFVInterface, ODFVInterface, SFVInterface };
