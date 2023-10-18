import { feast } from "../protos";

enum FEAST_FV_TYPES {
  regular = "regular",
  ondemand = "ondemand",
  stream = "stream"
}

interface regularFVInterface {
  name: string;
  type: FEAST_FV_TYPES.regular;
  features: feast.core.IFeatureSpecV2[];
  object: feast.core.IFeatureView;
}

interface ODFVInterface {
  name: string;
  type: FEAST_FV_TYPES.ondemand;
  features: feast.core.IOnDemandFeatureViewSpec[];
  object: feast.core.IOnDemandFeatureView;
}

interface SFVInterface {
  name: string;
  type: FEAST_FV_TYPES.stream;
  features: feast.core.IFeatureSpecV2[];
  object: feast.core.IStreamFeatureView;
}

type genericFVType = regularFVInterface | ODFVInterface | SFVInterface;

const mergedFVTypes = (objects: feast.core.Registry) => {
  const mergedFVMap: Record<string, genericFVType> = {};

  const mergedFVList: genericFVType[] = [];

  objects.featureViews?.forEach((fv) => {
    const obj: genericFVType = {
      name: fv.spec?.name!,
      type: FEAST_FV_TYPES.regular,
      features: fv.spec?.features!,
      object: fv,
    };

    mergedFVMap[fv.spec?.name!] = obj;
    mergedFVList.push(obj);
  });

  objects.onDemandFeatureViews?.forEach((odfv) => {
    const obj: genericFVType = {
      name: odfv.spec?.name!,
      type: FEAST_FV_TYPES.ondemand,
      features: odfv.spec?.features!,
      object: odfv,
    };

    mergedFVMap[odfv.spec?.name!] = obj;
    mergedFVList.push(obj);
  });

  objects.streamFeatureViews?.forEach((sfv) => {
    const obj: genericFVType = {
      name: sfv.spec?.name!,
      type: FEAST_FV_TYPES.stream,
      features: sfv.spec?.features!,
      object: sfv,
    };

    mergedFVMap[sfv.spec?.name!] = obj;
    mergedFVList.push(obj);
  });

  return { mergedFVMap, mergedFVList };
};

export default mergedFVTypes;
export { FEAST_FV_TYPES };
export type { genericFVType, regularFVInterface, ODFVInterface, SFVInterface };
