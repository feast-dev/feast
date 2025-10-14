//go:build integration

package http

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/server"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var httpServer *server.HttpServer
var getOnlineFeaturesRangeHandler http.HandlerFunc

func TestMain(m *testing.M) {
	err := test.SetupInitializedRepo("../")
	if err != nil {
		fmt.Printf("Failed to set up test environment: %v\n", err)
		os.Exit(1)
	}
	httpServer = server.GetHttpServer("../", "")

	// GetOnlineFeaturesRange Handler should be the second handler in the list returned by DefaultHttpHandlers
	for _, handler := range server.DefaultHttpHandlers(httpServer) {
		if handler.Path == "/get-online-features-range" {
			getOnlineFeaturesRangeHandler = handler.HandlerFunc.(http.HandlerFunc)
			break
		}
	}

	// Run the tests
	exitCode := m.Run()

	// Clean up the test environment
	test.CleanUpInitializedRepo("../")

	// Exit with the appropriate code
	if exitCode != 0 {
		fmt.Printf("CassandraOnlineStore HTTP Int Tests failed with exit code %d\n", exitCode)
	}
	os.Exit(exitCode)
}

func loadResponse(fileName string) ([]byte, error) {
	filePath, err := filepath.Abs("./" + fileName)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(filePath)
}

func TestGetOnlineFeaturesRange_Http(t *testing.T) {
	requestJson := []byte(`{
	  "features": [
		"all_dtypes_sorted:int_val",
		"all_dtypes_sorted:long_val",
		"all_dtypes_sorted:float_val",
		"all_dtypes_sorted:double_val",
		"all_dtypes_sorted:byte_val",
		"all_dtypes_sorted:string_val",
		"all_dtypes_sorted:timestamp_val",
		"all_dtypes_sorted:boolean_val",
		"all_dtypes_sorted:array_int_val",
		"all_dtypes_sorted:array_long_val",
		"all_dtypes_sorted:array_float_val",
		"all_dtypes_sorted:array_double_val",
		"all_dtypes_sorted:array_byte_val",
		"all_dtypes_sorted:array_string_val",
		"all_dtypes_sorted:array_timestamp_val",
		"all_dtypes_sorted:array_boolean_val",
		"all_dtypes_sorted:null_int_val",
		"all_dtypes_sorted:null_long_val",
		"all_dtypes_sorted:null_float_val",
		"all_dtypes_sorted:null_double_val",
		"all_dtypes_sorted:null_byte_val",
		"all_dtypes_sorted:null_string_val",
		"all_dtypes_sorted:null_timestamp_val",
		"all_dtypes_sorted:null_boolean_val",
		"all_dtypes_sorted:null_array_int_val",
		"all_dtypes_sorted:null_array_long_val",
		"all_dtypes_sorted:null_array_float_val",
		"all_dtypes_sorted:null_array_double_val",
		"all_dtypes_sorted:null_array_byte_val",
		"all_dtypes_sorted:null_array_string_val",
		"all_dtypes_sorted:null_array_timestamp_val",
		"all_dtypes_sorted:null_array_boolean_val",
		"all_dtypes_sorted:event_timestamp"
	  ],
	  "entities": {
		"index_id": [1, 2, 3]
	  },
	  "sort_key_filters": [
		{
		  "sort_key_name": "event_timestamp",
		  "range": {
			"range_start": 0
		  }
		}
	  ],
	  "limit": 10
	}`)

	request := httptest.NewRequest(http.MethodPost, "/get-online-features-range", bytes.NewBuffer(requestJson))
	responseRecorder := httptest.NewRecorder()

	getOnlineFeaturesRangeHandler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, responseRecorder.Code, http.StatusOK, "Expected HTTP status code 200 OK response body is: %s", responseRecorder.Body.String())
	expectedResponse, err := loadResponse("valid_response.json")
	require.NoError(t, err, "Failed to load expected response from file")
	assert.JSONEq(t, string(expectedResponse), responseRecorder.Body.String(), "Response body does not match expected JSON")
}

func TestGetOnlineFeaturesRange_Http_withOnlyEqualsFilter(t *testing.T) {
	requestJson := []byte(`{
	  "features": [
		"all_dtypes_sorted:int_val",
		"all_dtypes_sorted:long_val",
		"all_dtypes_sorted:float_val",
		"all_dtypes_sorted:double_val",
		"all_dtypes_sorted:byte_val",
		"all_dtypes_sorted:string_val",
		"all_dtypes_sorted:timestamp_val",
		"all_dtypes_sorted:boolean_val",
		"all_dtypes_sorted:array_int_val",
		"all_dtypes_sorted:array_long_val",
		"all_dtypes_sorted:array_float_val",
		"all_dtypes_sorted:array_double_val",
		"all_dtypes_sorted:array_byte_val",
		"all_dtypes_sorted:array_string_val",
		"all_dtypes_sorted:array_timestamp_val",
		"all_dtypes_sorted:array_boolean_val",
		"all_dtypes_sorted:null_int_val",
		"all_dtypes_sorted:null_long_val",
		"all_dtypes_sorted:null_float_val",
		"all_dtypes_sorted:null_double_val",
		"all_dtypes_sorted:null_byte_val",
		"all_dtypes_sorted:null_string_val",
		"all_dtypes_sorted:null_timestamp_val",
		"all_dtypes_sorted:null_boolean_val",
		"all_dtypes_sorted:null_array_int_val",
		"all_dtypes_sorted:null_array_long_val",
		"all_dtypes_sorted:null_array_float_val",
		"all_dtypes_sorted:null_array_double_val",
		"all_dtypes_sorted:null_array_byte_val",
		"all_dtypes_sorted:null_array_string_val",
		"all_dtypes_sorted:null_array_timestamp_val",
		"all_dtypes_sorted:null_array_boolean_val",
		"all_dtypes_sorted:event_timestamp"
	  ],
	  "entities": {
		"index_id": [2]
	  },
	  "sort_key_filters": [
		{
		  "sort_key_name": "event_timestamp",
		  "equals": 1744769171
		}
	  ],
	  "limit": 10
	}`)

	request := httptest.NewRequest(http.MethodPost, "/get-online-features-range", bytes.NewBuffer(requestJson))
	responseRecorder := httptest.NewRecorder()

	getOnlineFeaturesRangeHandler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, responseRecorder.Code, http.StatusOK, "Expected HTTP status code 200 OK response body is: %s", responseRecorder.Body.String())
	expectedResponse, err := loadResponse("valid_equals_response.json")
	require.NoError(t, err, "Failed to load expected response from file")
	assert.JSONEq(t, string(expectedResponse), responseRecorder.Body.String(), "Response body does not match expected JSON")
}

func TestGetOnlineFeaturesRange_Http_forNonExistentEntityKey(t *testing.T) {
	requestJson := []byte(`{
	  "features": [
		"all_dtypes_sorted:int_val",
		"all_dtypes_sorted:long_val",
		"all_dtypes_sorted:float_val",
		"all_dtypes_sorted:double_val",
		"all_dtypes_sorted:byte_val",
		"all_dtypes_sorted:string_val",
		"all_dtypes_sorted:timestamp_val",
		"all_dtypes_sorted:boolean_val",
		"all_dtypes_sorted:array_int_val",
		"all_dtypes_sorted:array_long_val",
		"all_dtypes_sorted:array_float_val",
		"all_dtypes_sorted:array_double_val",
		"all_dtypes_sorted:array_byte_val",
		"all_dtypes_sorted:array_string_val",
		"all_dtypes_sorted:array_timestamp_val",
		"all_dtypes_sorted:array_boolean_val",
		"all_dtypes_sorted:null_int_val",
		"all_dtypes_sorted:null_long_val",
		"all_dtypes_sorted:null_float_val",
		"all_dtypes_sorted:null_double_val",
		"all_dtypes_sorted:null_byte_val",
		"all_dtypes_sorted:null_string_val",
		"all_dtypes_sorted:null_timestamp_val",
		"all_dtypes_sorted:null_boolean_val",
		"all_dtypes_sorted:null_array_int_val",
		"all_dtypes_sorted:null_array_long_val",
		"all_dtypes_sorted:null_array_float_val",
		"all_dtypes_sorted:null_array_double_val",
		"all_dtypes_sorted:null_array_byte_val",
		"all_dtypes_sorted:null_array_string_val",
		"all_dtypes_sorted:null_array_timestamp_val",
		"all_dtypes_sorted:null_array_boolean_val",
		"all_dtypes_sorted:event_timestamp"
	  ],
	  "entities": {
		"index_id": [-1]
	  },
	  "sort_key_filters": [
		{
		  "sort_key_name": "event_timestamp",
		  "range": {
			"range_start": 0
		  }
		}
	  ],
	  "limit": 10
	}`)

	request := httptest.NewRequest(http.MethodPost, "/get-online-features-range", bytes.NewBuffer(requestJson))
	responseRecorder := httptest.NewRecorder()

	getOnlineFeaturesRangeHandler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, responseRecorder.Code, http.StatusOK, "Expected HTTP status code 200 OK response body is: %s", responseRecorder.Body.String())
	expectedResponse, err := loadResponse("valid_nonexistent_key_response.json")
	require.NoError(t, err, "Failed to load expected response from file")
	assert.JSONEq(t, string(expectedResponse), responseRecorder.Body.String(), "Response body does not match expected JSON")
}

func TestGetOnlineFeaturesRange_Http_includesDuplicatedRequestedFeatures(t *testing.T) {
	requestJson := []byte(`{
	  "features": [
		"all_dtypes_sorted:int_val",
		"all_dtypes_sorted:int_val"
	  ],
	  "entities": {
		"index_id": [1, 2, 3]
	  },
	  "sort_key_filters": [
		{
		  "sort_key_name": "event_timestamp",
		  "range": {
			"range_start": 0
		  }
		}
	  ],
	  "limit": 10
	}`)

	request := httptest.NewRequest(http.MethodPost, "/get-online-features-range", bytes.NewBuffer(requestJson))
	responseRecorder := httptest.NewRecorder()

	getOnlineFeaturesRangeHandler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, responseRecorder.Code, http.StatusOK, "Expected HTTP status code 200 OK response body is: %s", responseRecorder.Body.String())
	expectedResponse, err := loadResponse("valid_duplicate_features_response.json")
	require.NoError(t, err, "Failed to load expected response from file")
	assert.JSONEq(t, string(expectedResponse), responseRecorder.Body.String(), "Response body does not match expected JSON")
}

func TestGetOnlineFeaturesRange_Http_withEmptySortKeyFilter(t *testing.T) {
	requestJson := []byte(`{
	  "features": [
		"all_dtypes_sorted:int_val",
		"all_dtypes_sorted:long_val",
		"all_dtypes_sorted:float_val",
		"all_dtypes_sorted:double_val",
		"all_dtypes_sorted:byte_val",
		"all_dtypes_sorted:string_val",
		"all_dtypes_sorted:timestamp_val",
		"all_dtypes_sorted:boolean_val",
		"all_dtypes_sorted:array_int_val",
		"all_dtypes_sorted:array_long_val",
		"all_dtypes_sorted:array_float_val",
		"all_dtypes_sorted:array_double_val",
		"all_dtypes_sorted:array_byte_val",
		"all_dtypes_sorted:array_string_val",
		"all_dtypes_sorted:array_timestamp_val",
		"all_dtypes_sorted:array_boolean_val",
		"all_dtypes_sorted:null_int_val",
		"all_dtypes_sorted:null_long_val",
		"all_dtypes_sorted:null_float_val",
		"all_dtypes_sorted:null_double_val",
		"all_dtypes_sorted:null_byte_val",
		"all_dtypes_sorted:null_string_val",
		"all_dtypes_sorted:null_timestamp_val",
		"all_dtypes_sorted:null_boolean_val",
		"all_dtypes_sorted:null_array_int_val",
		"all_dtypes_sorted:null_array_long_val",
		"all_dtypes_sorted:null_array_float_val",
		"all_dtypes_sorted:null_array_double_val",
		"all_dtypes_sorted:null_array_byte_val",
		"all_dtypes_sorted:null_array_string_val",
		"all_dtypes_sorted:null_array_timestamp_val",
		"all_dtypes_sorted:null_array_boolean_val",
		"all_dtypes_sorted:event_timestamp"
	  ],
	  "entities": {
		"index_id": [1, 2, 3]
	  },
	  "sort_key_filters": [],
	  "limit": 10
	}`)

	request := httptest.NewRequest(http.MethodPost, "/get-online-features-range", bytes.NewBuffer(requestJson))
	responseRecorder := httptest.NewRecorder()

	getOnlineFeaturesRangeHandler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, responseRecorder.Code, http.StatusOK, "Expected HTTP status code 200 OK response body is: %s", responseRecorder.Body.String())
	expectedResponse, err := loadResponse("valid_response.json")
	require.NoError(t, err, "Failed to load expected response from file")
	assert.JSONEq(t, string(expectedResponse), responseRecorder.Body.String(), "Response body does not match expected JSON")
}

func TestGetOnlineFeaturesRange_Http_withFeatureService(t *testing.T) {
	requestJson := []byte(`{
	  "feature_service": "test_sorted_service",
	  "entities": {
		"index_id": [1, 2, 3]
	  },
	  "sort_key_filters": [
		{
		  "sort_key_name": "event_timestamp",
		  "range": {
			"range_start": 0
		  }
		}
	  ],
	  "limit": 10
	}`)

	request := httptest.NewRequest(http.MethodPost, "/get-online-features-range", bytes.NewBuffer(requestJson))
	responseRecorder := httptest.NewRecorder()

	getOnlineFeaturesRangeHandler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, responseRecorder.Code, http.StatusOK, "Expected HTTP status code 200 OK response body is: %s", responseRecorder.Body.String())
	expectedResponse, err := loadResponse("valid_response.json")
	require.NoError(t, err, "Failed to load expected response from file")
	assert.JSONEq(t, string(expectedResponse), responseRecorder.Body.String(), "Response body does not match expected JSON")
}

func TestGetOnlineFeaturesRange_Http_withInvalidFeatureService(t *testing.T) {
	requestJson := []byte(`{
        "feature_service": "invalid_service",
        "entities": {
            "index_id": [1, 2, 3]
        },
        "sort_key_filters": [
            {
                "sort_key_name": "event_timestamp",
                "range": {
                    "range_start": 0
                }
            }
        ],
        "limit": 10
    }`)

	request := httptest.NewRequest(http.MethodPost, "/get-online-features-range", bytes.NewBuffer(requestJson))
	responseRecorder := httptest.NewRecorder()

	getOnlineFeaturesRangeHandler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, http.StatusNotFound, responseRecorder.Code)
	assert.Contains(t, responseRecorder.Body.String(), "Error getting feature service from registry", "Response body does not contain expected error message")
}

func TestGetOnlineFeaturesRange_Http_withInvalidSortedFeatureView(t *testing.T) {
	requestJson := []byte(`{
        "features": ["invalid_sorted_view:some_feature"],
        "entities": {
            "index_id": [1, 2, 3]
        },
        "sort_key_filters": [
            {
                "sort_key_name": "event_timestamp",
                "range": {
                    "range_start": {
                        "unix_timestamp_val": 0
                    }
                }
            }
        ],
        "limit": 10
    }`)

	request := httptest.NewRequest(http.MethodPost, "/get-online-features-range", bytes.NewBuffer(requestJson))
	responseRecorder := httptest.NewRecorder()

	getOnlineFeaturesRangeHandler.ServeHTTP(responseRecorder, request)
	assert.Equal(t, http.StatusBadRequest, responseRecorder.Code)
	expectedErrorMessage := `{"error":"sorted feature view invalid_sorted_view doesn't exist, please make sure that you have created the sorted feature view invalid_sorted_view and that you have registered it by running \"apply\"","status_code":400}`
	assert.JSONEq(t, expectedErrorMessage, responseRecorder.Body.String(), "Response body does not match expected error message")
}

func TestGetOnlineFeaturesRange_Http_withInvalidSortKeyFilter(t *testing.T) {
	testCases := []struct {
		request        string
		expectedStatus int
		expectedError  string
	}{
		{
			request: `{
			  "features": [
				"all_dtypes_sorted:int_val"
			  ],
			  "entities": {
				"index_id": [1, 2, 3]
			  },
			  "sort_key_filters": [
				{
				  "sort_key_name": "event_timestamp",
				  "range": {
					"range_start": "invalid_value"
				  }
				}
			  ],
			  "limit": 10
			}`,
			expectedStatus: http.StatusBadRequest,
			expectedError:  `{"error":"error converting sort key filter range start for event_timestamp: unsupported value type for conversion: UNIX_TIMESTAMP for actual value type: *types.Value_StringVal","status_code":400}`,
		},
		{
			request: `{
			  "features": [
				"all_dtypes_sorted:int_val"
			  ],
			  "entities": {
				"index_id": [1, 2, 3]
			  },
			  "sort_key_filters": [
				{
				  "sort_key_name": "event_timestamp",
				  "range": {
					"range_end": 10.45
				  }
				}
			  ],
			  "limit": 10
			}`,
			expectedStatus: http.StatusBadRequest,
			expectedError:  `{"error":"error converting sort key filter range end for event_timestamp: unsupported value type for conversion: UNIX_TIMESTAMP for actual value type: *types.Value_DoubleVal","status_code":400}`,
		},
		{
			request: `{
			  "features": [
				"all_dtypes_sorted:int_val"
			  ],
			  "entities": {
				"index_id": [1, 2, 3]
			  },
			  "sort_key_filters": [
				{
				  "sort_key_name": "event_timestamp",
				  "equals": "invalid_value"
				}
			  ],
			  "limit": 10
			}`,
			expectedStatus: http.StatusBadRequest,
			expectedError:  `{"error":"error converting sort key filter equals for event_timestamp: unsupported value type for conversion: UNIX_TIMESTAMP for actual value type: *types.Value_StringVal","status_code":400}`,
		},
		{
			request: `{
			  "features": [
				"all_dtypes_sorted:int_val"
			  ],
			  "entities": {
				"index_id": [1, 2, 3]
			  },
			  "sort_key_filters": [
				{
				  "sort_key_name": "event_timestamp",
				  "equals": {}
				}
			  ],
			  "limit": 10
			}`,
			expectedStatus: http.StatusBadRequest,
			expectedError:  `{"error":"error parsing equals filter: could not parse JSON value: {}","status_code":400}`,
		},
	}

	for _, tc := range testCases {
		request := httptest.NewRequest(http.MethodPost, "/get-online-features-range", bytes.NewBuffer([]byte(tc.request)))
		responseRecorder := httptest.NewRecorder()

		getOnlineFeaturesRangeHandler.ServeHTTP(responseRecorder, request)
		assert.Equal(t, tc.expectedStatus, responseRecorder.Code)
		assert.Equal(t, tc.expectedError, responseRecorder.Body.String(), "Response body does not match expected error message")
	}
}
