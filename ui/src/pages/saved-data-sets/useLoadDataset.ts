import { useParams } from "react-router-dom";
import useResourceQuery, {
  savedDatasetDetailPath,
} from "../../queries/useResourceQuery";

const useLoadDataset = (datasetName: string) => {
  const { projectName } = useParams();

  return useResourceQuery<any>({
    resourceType: `saved-dataset:${datasetName}`,
    project: projectName,
    protoSelect: (d) =>
      d.objects.savedDatasets?.find(
        (sd: any) => sd.spec?.name === datasetName,
      ),
    restPath: savedDatasetDetailPath(datasetName, projectName || ""),
    restSelect: (d) => d,
    enabled: !!datasetName,
  });
};

export default useLoadDataset;
