package feast.core.training;

import com.google.protobuf.Timestamp;
import feast.core.DatasetServiceProto;

import java.util.Map;

public interface TrainingDatasetCreator {
	/**
	 * Create a training dataset for a feature set for features created between startDate (inclusive)
	 * and endDate (inclusive)
	 *
	 * @param featureSet feature set for which the training dataset should be created
	 * @param startDate starting date of the training dataset (inclusive)
	 * @param endDate end date of the training dataset (inclusive)
	 * @param limit maximum number of row should be created.
	 * @param namePrefix prefix for dataset name
	 * @param filters additional where clause
	 * @return dataset info associated with the created training dataset
	 */
	DatasetServiceProto.DatasetInfo createDataset(
		DatasetServiceProto.FeatureSet featureSet,
		Timestamp startDate,
		Timestamp endDate,
		long limit,
		String namePrefix,
		Map<String, String> filters);
}
