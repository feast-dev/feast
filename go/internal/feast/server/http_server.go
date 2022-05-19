package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/server/logging"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
	"net/http"
	"time"
)

type httpServer struct {
	fs             *feast.FeatureStore
	loggingService *logging.LoggingService
	server         *http.Server
}

// Some Feast types aren't supported during JSON conversion
type repeatedValue struct {
	stringVal     []string
	int64Val      []int64
	doubleVal     []float64
	boolVal       []bool
	stringListVal [][]string
	int64ListVal  [][]int64
	doubleListVal [][]float64
	boolListVal   [][]bool
}

func (u *repeatedValue) UnmarshalJSON(data []byte) error {
	isString := false
	isDouble := false
	isInt64 := false
	isArray := false
	openBraketCounter := 0
	for _, b := range data {
		if b == '"' {
			isString = true
		}
		if b == '.' {
			isDouble = true
		}
		if b >= '0' && b <= '9' {
			isInt64 = true
		}
		if b == '[' {
			openBraketCounter++
			if openBraketCounter > 1 {
				isArray = true
			}
		}
	}
	var err error
	if !isArray {
		if isString {
			err = json.Unmarshal(data, &u.stringVal)
		} else if isDouble {
			err = json.Unmarshal(data, &u.doubleVal)
		} else if isInt64 {
			err = json.Unmarshal(data, &u.int64Val)
		} else {
			err = json.Unmarshal(data, &u.boolVal)
		}
	} else {
		if isString {
			err = json.Unmarshal(data, &u.stringListVal)
		} else if isDouble {
			err = json.Unmarshal(data, &u.doubleListVal)
		} else if isInt64 {
			err = json.Unmarshal(data, &u.int64ListVal)
		} else {
			err = json.Unmarshal(data, &u.boolListVal)
		}
	}
	return err
}

func (u *repeatedValue) ToProto() *prototypes.RepeatedValue {
	proto := new(prototypes.RepeatedValue)
	if u.stringVal != nil {
		for _, val := range u.stringVal {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_StringVal{StringVal: val}})
		}
	}
	if u.int64Val != nil {
		for _, val := range u.int64Val {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_Int64Val{Int64Val: val}})
		}
	}
	if u.doubleVal != nil {
		for _, val := range u.doubleVal {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_DoubleVal{DoubleVal: val}})
		}
	}
	if u.boolVal != nil {
		for _, val := range u.boolVal {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_BoolVal{BoolVal: val}})
		}
	}
	if u.stringListVal != nil {
		for _, val := range u.stringListVal {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_StringListVal{StringListVal: &prototypes.StringList{Val: val}}})
		}
	}
	if u.int64ListVal != nil {
		for _, val := range u.int64ListVal {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_Int64ListVal{Int64ListVal: &prototypes.Int64List{Val: val}}})
		}
	}
	if u.doubleListVal != nil {
		for _, val := range u.doubleListVal {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_DoubleListVal{DoubleListVal: &prototypes.DoubleList{Val: val}}})
		}
	}
	if u.boolListVal != nil {
		for _, val := range u.boolListVal {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_BoolListVal{BoolListVal: &prototypes.BoolList{Val: val}}})
		}
	}
	return proto
}

type getOnlineFeaturesRequest struct {
	FeatureService   *string                  `json:"feature_service"`
	Features         []string                 `json:"features"`
	Entities         map[string]repeatedValue `json:"entities"`
	FullFeatureNames bool                     `json:"full_feature_names"`
	RequestContext   map[string]repeatedValue `json:"request_context"`
}

func NewHttpServer(fs *feast.FeatureStore, loggingService *logging.LoggingService) *httpServer {
	return &httpServer{fs: fs, loggingService: loggingService}
}

func (s *httpServer) getOnlineFeatures(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.NotFound(w, r)
		return
	}

	decoder := json.NewDecoder(r.Body)
	var request getOnlineFeaturesRequest
	err := decoder.Decode(&request)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error decoding JSON request data: %+v", err), http.StatusInternalServerError)
		return
	}
	var featureService *model.FeatureService
	if request.FeatureService != nil {
		featureService, err = s.fs.GetFeatureService(*request.FeatureService)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error getting feature service from registry: %+v", err), http.StatusInternalServerError)
			return
		}
	}
	entitiesProto := make(map[string]*prototypes.RepeatedValue)
	for key, value := range request.Entities {
		entitiesProto[key] = value.ToProto()
	}
	requestContextProto := make(map[string]*prototypes.RepeatedValue)
	for key, value := range request.RequestContext {
		requestContextProto[key] = value.ToProto()
	}

	featureVectors, err := s.fs.GetOnlineFeatures(
		r.Context(),
		request.Features,
		featureService,
		entitiesProto,
		requestContextProto,
		request.FullFeatureNames)

	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting feature vector: %+v", err), http.StatusInternalServerError)
		return
	}

	var featureNames []string
	var results []map[string]interface{}
	for _, vector := range featureVectors {
		featureNames = append(featureNames, vector.Name)
		result := make(map[string]interface{})
		var statuses []string
		for _, status := range vector.Statuses {
			statuses = append(statuses, status.String())
		}
		var timestamps []string
		for _, timestamp := range vector.Timestamps {
			timestamps = append(timestamps, timestamp.AsTime().Format(time.RFC3339))
		}

		result["statuses"] = statuses
		result["event_timestamps"] = timestamps
		// Note, that vector.Values is an Arrow Array, but this type implements JSON Marshaller.
		// So, it's not necessary to pre-process it in any way.
		result["values"] = vector.Values

		results = append(results, result)
	}

	response := map[string]interface{}{
		"metadata": map[string]interface{}{
			"feature_names": featureNames,
		},
		"results": results,
	}

	err = json.NewEncoder(w).Encode(response)

	if err != nil {
		http.Error(w, fmt.Sprintf("Error encoding response: %+v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if featureService != nil && featureService.LoggingConfig != nil && s.loggingService != nil {
		logger, err := s.loggingService.GetOrCreateLogger(featureService)
		if err != nil {
			http.Error(w, fmt.Sprintf("Couldn't instantiate logger for feature service %s: %+v", featureService.Name, err), http.StatusInternalServerError)
			return
		}

		requestId := GenerateRequestId()

		// Note: we're converting arrow to proto for feature logging. In the future we should
		// base feature logging on arrow so that we don't have to do this extra conversion.
		var featureVectorProtos []*serving.GetOnlineFeaturesResponse_FeatureVector
		for _, vector := range featureVectors[len(request.Entities):] {
			values, err := types.ArrowValuesToProtoValues(vector.Values)
			if err != nil {
				http.Error(w, fmt.Sprintf("Couldn't convert arrow values into protobuf: %+v", err), http.StatusInternalServerError)
				return
			}
			featureVectorProtos = append(featureVectorProtos, &serving.GetOnlineFeaturesResponse_FeatureVector{
				Values:          values,
				Statuses:        vector.Statuses,
				EventTimestamps: vector.Timestamps,
			})
		}

		err = logger.Log(entitiesProto, featureVectorProtos, featureNames[len(request.Entities):], requestContextProto, requestId)
		if err != nil {
			http.Error(w, fmt.Sprintf("LoggerImpl error[%s]: %+v", featureService.Name, err), http.StatusInternalServerError)
			return
		}
	}
}

func (s *httpServer) Serve(host string, port int) error {
	s.server = &http.Server{Addr: fmt.Sprintf("%s:%d", host, port), Handler: nil}
	http.HandleFunc("/get-online-features", s.getOnlineFeatures)
	err := s.server.ListenAndServe()
	// Don't return the error if it's caused by graceful shutdown using Stop()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
func (s *httpServer) Stop() error {
	if s.server != nil {
		return s.server.Shutdown(context.Background())
	}
	return nil
}
