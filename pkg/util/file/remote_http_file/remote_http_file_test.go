package file

// type TestCase struct {
// 	name         string
// 	path         string
// 	status       int
// 	responder    func() ([]byte, map[string]string)
// 	expectedData []byte
// 	expectedErr  string
// }
//
// func TestRemoteHttpFile_DoFetch(t *testing.T) {
//
// 	memorizedNow := time.Now()
// 	etag := sha1.Sum([]byte(memorizedNow.String()))
//
// 	tests := []TestCase{
// 		{
// 			name:   "Simple json",
// 			path:   "/simple_json",
// 			status: http.StatusOK,
// 			responder: func() ([]byte, map[string]string) {
// 				return []byte(fmt.Sprintf("{\"time\":%d}", time.Now().Unix())), nil
// 			},
// 			expectedData: nil,
// 		},
//
// 		{
// 			name:   "Simple json with last update",
// 			path:   "/simple_json_with_last_update",
// 			status: http.StatusOK,
// 			responder: func() ([]byte, map[string]string) {
// 				headers := map[string]string{
// 					"Last-Modified": memorizedNow.Format(http.TimeFormat),
// 				}
// 				return []byte(fmt.Sprintf("{\"time\":%d}", memorizedNow.Unix())), headers
// 			},
// 			expectedData: []byte(fmt.Sprintf("{\"time\":%d}", memorizedNow.Unix())),
// 		},
//
// 		{
// 			name:   "Simple json with ETag",
// 			path:   "/simple_json_with_etag",
// 			status: http.StatusOK,
// 			responder: func() ([]byte, map[string]string) {
// 				headers := map[string]string{
// 					"ETag": fmt.Sprintf("%d", etag),
// 				}
// 				return []byte(fmt.Sprintf("{\"time\":%d}", memorizedNow.Unix())), headers
// 			},
// 			expectedData: []byte(fmt.Sprintf("{\"time\":%d}", memorizedNow.Unix())),
// 		},
//
// 		{
// 			name:        "With not modified",
// 			path:        "/with_not_modified",
// 			status:      http.StatusNotModified,
// 			responder:   nil,
// 			expectedErr: "there is no previous result yet",
// 		},
//
// 		{
// 			name:         "With not found",
// 			path:         "/with_not_found",
// 			status:       http.StatusNotFound,
// 			responder:    nil,
// 			expectedData: nil,
// 			expectedErr:  "Bad response status: 404",
// 		},
//
// 		{
// 			name:         "With failure",
// 			path:         "/with_failure",
// 			status:       http.StatusInternalServerError,
// 			responder:    nil,
// 			expectedData: nil,
// 			expectedErr:  "Bad response status: 500",
// 		},
// 	}
//
// 	mux := http.NewServeMux()
//
// 	generatedData := make(map[string][]byte)
//
// 	for _, tt := range tests {
// 		func(testCase TestCase) {
// 			mux.HandleFunc(testCase.path, func(w http.ResponseWriter, r *http.Request) {
// 				w.WriteHeader(testCase.status)
// 				if testCase.responder != nil {
// 					body, headers := testCase.responder()
// 					if headers != nil {
// 						for k, v := range headers {
// 							w.Header().Set(k, v)
// 						}
// 					}
// 					if body != nil {
// 						generatedData[testCase.name] = body
// 						w.Write(body)
// 					}
// 				}
// 			})
// 		}(tt)
// 	}
//
// 	go func() { http.ListenAndServe(":8080", mux) }()
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
//
// 			rhf, err := NewWithInterval(
// 				fmt.Sprintf("http://localhost:8080%s", tt.path),
// 				100*time.Millisecond,
// 			)
// 			if err != nil {
// 				t.Fatalf("Failed to initialize a new RemoteHttpFile: %s", err)
// 			}
// 			if err := rhf.Deploy(); err != nil {
// 				t.Fatalf("Failed to deploy a RemoteHttpFile: %s", err)
// 			}
// 			defer rhf.TearDown()
//
// 			gotData, err := rhf.ReadRawData()
//
// 			var wantData []byte
// 			if tt.expectedData != nil {
// 				wantData = tt.expectedData
// 			} else if v, ok := generatedData[tt.name]; ok {
// 				wantData = v
// 			}
// 			t.Logf("Expected response: %s", wantData)
//
// 			if bytes.Compare(gotData, wantData) != 0 {
// 				t.Errorf("Unexpected content was returned by the file: %s, want: %s",
// 					gotData, wantData)
// 			}
//
// 			if err != nil || tt.expectedErr != "" {
// 				if tt.expectedErr != "" {
// 					if err == nil {
// 						t.Errorf("Expected error like \"%s\", but got none", tt.expectedErr)
// 					} else if match, err := regexp.Match(tt.expectedErr, []byte(err.Error())); err != nil {
// 						t.Fatalf("Failed to match regex using pattern: %s: %s",
// 							tt.expectedErr, err)
// 					} else if !match {
// 						t.Errorf("Unknown error while reading the file: %s, expected: %s",
// 							err, tt.expectedErr)
// 					}
// 				} else {
// 					t.Errorf("Unexpected error while reading the file: %s", err)
// 				}
// 			}
// 		})
// 	}
// }
