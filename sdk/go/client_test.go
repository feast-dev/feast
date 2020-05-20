package feast

import (
	"context"
	"testing"

	"github.com/feast-dev/feast/sdk/go/mocks"
	"github.com/feast-dev/feast/sdk/go/protos/feast/serving"
	"github.com/feast-dev/feast/sdk/go/protos/feast/types"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/opentracing/opentracing-go"
)

func TestGetOnlineFeatures(t *testing.T) {
	tt := []struct {
		name    string
		req     OnlineFeaturesRequest
		recieve OnlineFeaturesResponse
		want    OnlineFeaturesResponse
		wantErr bool
		err     error
	}{
		{
			name: "valid",
			req: OnlineFeaturesRequest{
				Features: []string{
					"driver:rating",
					"rating",
				},
				Entities: []Row{
					{"driver_id": Int64Val(1)},
				},
				Project: "driver_project",
			},
			// check GetOnlineFeatures() should strip projects returned from serving
			recieve: OnlineFeaturesResponse{
				RawResponse: &serving.GetOnlineFeaturesResponse{
					FieldValues: []*serving.GetOnlineFeaturesResponse_FieldValues{
						{
							Fields: map[string]*types.Value{
								"driver_project/driver:rating": Int64Val(1),
								"driver_project/rating":        Int64Val(1),
							},
						},
					},
				},
			},
			want: OnlineFeaturesResponse{
				RawResponse: &serving.GetOnlineFeaturesResponse{
					FieldValues: []*serving.GetOnlineFeaturesResponse_FieldValues{
						{
							Fields: map[string]*types.Value{
								"driver:rating": Int64Val(1),
								"rating":        Int64Val(1),
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			// mock feast grpc client get online feature requestss
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			cli := mock_serving.NewMockServingServiceClient(ctrl)
			ctx := context.Background()
			_, traceCtx := opentracing.StartSpanFromContext(ctx, "get_online_features")
			rawRequest, _ := tc.req.buildRequest()
			resp := tc.recieve.RawResponse
			cli.EXPECT().GetOnlineFeatures(traceCtx, rawRequest).Return(resp, nil).Times(1)

			client := &GrpcClient{
				cli: cli,
			}
			got, err := client.GetOnlineFeatures(ctx, &tc.req)

			if err != nil && !tc.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if tc.wantErr && err.Error() != tc.err.Error() {
				t.Errorf("error = %v, expected err = %v", err, tc.err)
				return
			}
			// TODO: compare directly once OnlineFeaturesResponse no longer embeds a rawResponse.
			if !cmp.Equal(got.RawResponse.String(), tc.want.RawResponse.String()) {
				t.Errorf("got: \n%v\nwant:\n%v", got.RawResponse.String(), tc.want.RawResponse.String())
			}
		})
	}
}
