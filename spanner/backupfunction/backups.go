// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [START spanner_functions_backup_util]

// Package backupfunction... TODO
package backupfunction

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"
"strings"
	// "cloud.google.com/go/spanner"
	// "google.golang.org/api/iterator"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
)

// client is a global Spanner client, to avoid initializing a new client for
// every request.
var client *database.DatabaseAdminClient
var clientOnce sync.Once

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// db is the name of the database to query.
var db = "projects/my-project/instances/my-instance/databases/example-db"

//TODO write handler for pubsub
// // SpannerBackup is...
// func Backupfunct(ctx context.Context, m PubSubMessage) error {
// 	clientOnce.Do(func() {
// 		// Declare a separate err variable to avoid shadowing client.
// 		var err error
// 		client, err = database.NewDatabaseAdminClient(context.Background())
// 		if err != nil {
// 			log.Printf("database.NewDatabaseAdminClient: %v", err)
// 			return 
// 		}

// 		_, backupErr := CreateBackup(ctx, w, client, *database, expiry, backupPrefix)
// 	})
// 	return nil
// }

// [END spanner_functions_backup_util]

// CreateBackup calls StartBackupOperation
func CreateBackup(ctx context.Context, w io.Writer, adminClient *database.DatabaseAdminClient, database string, expiry time.Duration, backupPrefix string) (lrop *database.CreateBackupOperation, err error) {
	timeNow := time.Now()
	backupID := backupPrefix + strings.Replace(timeNow.Format("20060102150405.000000000"), ".", "", -1)
	fmt.Fprintf(w, "backupID = %s\n", backupID)
	expires := timeNow.Add(expiry)
	op, err := adminClient.StartBackupOperation(ctx, backupID, database, expires)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(w, "Create backup operation [%s] started for database [%s], set to expire at [%s], backupID = %s\n", op.Name(), database, expires.Format(time.RFC3339), backupID)
	return op, nil
}
