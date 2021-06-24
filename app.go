package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

// Custom structs to hold data from `Archives/edgar/full-index`
type EdgarData struct {
	Directory Directory `json:"directory"`
}

type Directory struct {
	Item []Item `json:"item"`
	Name string `json:"name"`
}

type Item struct {
	LastModified string `json:"last-modified"`
	Name         string `json:"name"`
	Type         string `json:"type"`
	Href         string `json:"href"`
	Size         string `json:"size"`
}

// Custom struct for data that will go into BigQuery
type IndexData struct {
	Id        string `json:"id"`
	Year      string `json:"year"`
	Quarter   string `json:"quarter"`
	Index     string `json:"index"`
	IndexType string `json:"type"`
}

// SEC EDGAR base url
const baseUrl string = "https://www.sec.gov/Archives/edgar"

// The GCP Project Id
const projectID string = "alis-recruiting-7466497"

// The GCS Bucket Name
const bucketName string = "sec-edgar-files"

// The BigQuery Dataset Id
const datasetID string = "sec_edgar_files"

// Define a wait group that will help in synchronization of goroutines
var waitGroup sync.WaitGroup

// Runs after global variables have been initialized. We can use this to set a few things before the program runs
func init() {
	// Set the GOOGLE_APPLICATION_CREDENTIALS environment variable that is required to authenticate to Google APIs. We point to our credentials file. From my experience with other languages, Ideally you would want to load environment variables from a .env file. For the simplicity of this, we'll set it manually
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")

	fmt.Println("GOOGLE_APPLICATION_CREDENTIALS has been set")
}

func main() {
	// Process data from /full-index
	processFullIndex()
}

func processFullIndex() {
	// Full index json data file
	url := baseUrl + "/full-index/index.json"

	// Variable that will store data we get back from making a call to url above
	indices := EdgarData{}

	// Variable that stores all directories returned by the request to url above
	directories := []Item{}

	// Make a call to url above and unmarshal result into indices. The function returns an error
	jsonErr := getJson(url, &indices)

	// Check for error and stop execution. Might be more ideal to use panic and recover
	if jsonErr != nil {
		log.Fatal(jsonErr)
	}

	// Iterate through result from url above
	for _, item := range indices.Directory.Item {
		// Check if item is a directory and add it to directories variable.
		if item.Type == "dir" {
			directories = append(directories, item)
		}
	}

	// Make a channel that will handle our processData goroutine. Give it a buffer of length of directories multiplied by the number of quarters
	urisChannel := make(chan string, len(directories)*4)

	// Iterate over directories
	for _, item := range directories {
		// We have 4 quarters each year. We loop through those.
		for i := 0; i < 4; i++ {
			// Deferred function that will recover in case our processData function panics out
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Panic error is %v", r)
				}
			}()

			// Increment the wait group delta so that we can add a goroutine
			waitGroup.Add(1)

			// Process our data in a goroutine
			go processData(urisChannel, item.Name, "QTR"+strconv.Itoa(i+1))
		}
	}

	// Wait for all the deltas in our waitgroup to reach 0
	waitGroup.Wait()
	// Close our channel
	close(urisChannel)
	fmt.Println("Done with GCS wait group. Getting ready for BigQuery")

	// Define table schema for BigQuery
	tableSchema := bigquery.Schema{
		{Name: "id", Type: bigquery.StringFieldType},
		{Name: "year", Type: bigquery.IntegerFieldType},
		{Name: "quarter", Type: bigquery.StringFieldType},
		{Name: "index", Type: bigquery.StringFieldType},
		{Name: "type", Type: bigquery.StringFieldType},
	}

	// Iterate over all GCS uris passed through the channel
	for uri := range urisChannel {
		// Deferred function that will recover in case our insertIntoBigQueryTable function panics out
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Panic error is %v", r)
			}
		}()

		// Increment the wait group delta so that we can add a goroutine
		waitGroup.Add(1)

		// Insert row into BigQuery table in a goroutine
		go insertIntoBigQueryTable(projectID, datasetID, "indices", uri, tableSchema)
	}

	// Wait for all the deltas in our waitgroup to reach 0
	waitGroup.Wait()
	fmt.Println("Done With BigQuery")
}

// This function takes in a channel to send data over, year of filing, quarter of filing. The function processes our data into a json schema that we can store on GCS and load into BigQuery
func processData(urisChannel chan string, year string, quarter string) {
	// Decrement the wait group delta
	defer waitGroup.Done()

	// Place index types we need to collect into an Array of fixed length
	indices := [4]string{"company", "form", "master", "xbrl"}

	// Iterate through our indices
	for _, item := range indices {
		// Item Id, A combination of the year, quarter and actual index type
		itemId := year + "/" + quarter + "/" + item + ".idx"
		// GCS file name, Name/Path of Json file that will be eventually loaded into BigQuery
		remoteFileName := year + "/" + quarter + "/" + item + ".json"

		// Load our data into our IndexData struct
		indexData := IndexData{
			Id:        itemId,
			Year:      year,
			Quarter:   quarter,
			Index:     baseUrl + "/full-index/" + itemId,
			IndexType: item,
		}

		// Marshal our IndexData struct into a string that we can write into a GCS object
		indexDataString, err := json.Marshal(indexData)

		// Panic out if there's an error
		if err != nil {
			panic(err)
		}

		// Create a buffer which contains a Write and WriteTo methods
		buf := new(bytes.Buffer)

		// Upload the JSON file into GCS. Pass the Buffer, Bucket name, index data and the GCS file path. The method returns an error. It might be ideal to use panic and recover
		uploadErr := uploadFileToGCS(buf, bucketName, string(indexDataString), remoteFileName)

		// Check if there's an error
		if uploadErr != nil {
			// Check the type of error thrown
			switch e := uploadErr.(type) {

			// Handle the error if it's a google api error
			case *googleapi.Error:
				{
					// Check if the error is because one of the preconditions set on GCS upload fails. The precondition that we've set checks if the file already exists. If the file/object exists, we just skip and end execution of the function
					if e.Code == http.StatusPreconditionFailed {
						fmt.Printf("Item %v already exists, No need for upload\n\n", itemId)
						return
					}
				}

			default:
				{
					// Panic out if the error is not a google api error
					panic(uploadErr)
				}
			}
			// Panic out if the error is not a google api error or due to a set precondition
			panic(uploadErr)
		}

		// GCS Uri to the uploaded file
		link := "gs://" + bucketName + "/" + remoteFileName

		fmt.Printf("Uploaded item: %v\n\n", itemId)

		// Pass the link through our channel
		urisChannel <- link
	}
}

// This function makes a http request to the specified url and Unmarshals the error into the passed pointer.
func getJson(url string, target interface{}) error {

	// Create a new http client with a timeout
	var httpClient = &http.Client{Timeout: time.Second * 10}

	// Create a new http request
	req, err := http.NewRequest(http.MethodGet, url, nil)
	// Stop execution in case of an error. Might be ideal to use panic and recover
	if err != nil {
		log.Fatal(err)
	}

	// Make a request using the http client
	res, getErr := httpClient.Do(req)
	if getErr != nil {
		log.Fatal(getErr)
	}

	// Checks if the response was successful. If not, stops execution
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		log.Fatal(res.Status)
	}

	// Checks if there's a body in the response and closes the body when the rest of the code executes
	if res.Body != nil {
		defer res.Body.Close()
	}

	// Read the contents of the body
	body, readErr := ioutil.ReadAll(res.Body)
	if readErr != nil {
		log.Fatal(readErr)
	}

	// Unmarshal the contents of the body into the passed pointer
	return json.Unmarshal(body, target)

}

// This function uploads a file into GCS. The function takes in an object with a Write and WriteTo method such as a Buffer, a GCS Bucket name, The file's content as a string and the GCS Object path
func uploadFileToGCS(w io.Writer, bucketName, localObject string, remoteObjectPath string) error {
	// Create a context for use with the GCS client
	ctx := context.Background()

	// Create a new GCS client
	client, err := storage.NewClient(ctx)

	// Check if there was any error while creating the client
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	// Close the client once everything else is done
	defer client.Close()

	// Create timeout for the clients context
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	// Cancel the timeout if everything runs
	defer cancel()

	// Create a reference to the GCS bucket
	bucket := client.Bucket(bucketName)

	// Create a reference to the GCS Object
	object := bucket.Object(remoteObjectPath)

	// Create a new GCS writer. We also set a precondition on the object to check if the file already exists
	storageWriter := object.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)

	// Upload an object with storage.Writer. If there's an error while uploading, return the error
	if _, err = io.WriteString(storageWriter, localObject); err != nil {
		return fmt.Errorf("io.WriteString: %v", err)
	}

	// Close storage writer. Return an error if any arises
	if err := storageWriter.Close(); err != nil {

		return err
	}

	// Writes to our buffer that the file has been uploaded
	fmt.Fprintf(w, "Blob %v uploaded.\n", remoteObjectPath)
	return nil
}

// This function inserts a row into Google BigQuery from a GCS json file. Takes in the GCP Project id, The BigQuery dataset id, the table id, GCS json file uri and the schema
func insertIntoBigQueryTable(projectID, datasetID, tableID string, gcsUri string, schema bigquery.Schema) {
	// Decrement the wait group delta once everything has run
	defer waitGroup.Done()

	// Create a new context for the big query client
	ctx := context.Background()

	// Create a new big query client
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Create a reference to the gcs json file
	gcsRef := bigquery.NewGCSReference(gcsUri)
	// Set the file's format at JSON
	gcsRef.SourceFormat = bigquery.JSON
	// Set the schema to the one passed via the parameters
	gcsRef.Schema = schema

	// Create a reference to our dataset
	dataset := client.Dataset(datasetID)

	// Create a reference to our table
	table := dataset.Table(tableID)

	// Create a loader for the table from the GCS json file reference
	loader := table.LoaderFrom(gcsRef)

	// Set the loader's write deposition to append. This will ensure that the new row is appended to the table
	loader.WriteDisposition = bigquery.WriteAppend

	// Create a new job using our table loader
	job, err := loader.Run(ctx)
	if err != nil {
		panic(err)
	}

	// Wait for the job to complete running.  Panic out if any error arises
	status, err := job.Wait(ctx)
	if err != nil {
		panic(err)
	}

	// Panic out if there's an error while waiting for the job to finish
	if status.Err() != nil {
		panic(err)
	}

	fmt.Printf("Done with big query load for %v\n", gcsUri)
}

// func createDataset(projectID, datasetID string) (string, error) {

// 	ctx := context.Background()

// 	client, err := bigquery.NewClient(ctx, projectID)
// 	if err != nil {
// 		return "", fmt.Errorf("bigquery.NewClient: %v", err)
// 	}
// 	defer client.Close()

// 	metadata := &bigquery.DatasetMetadata{
// 		Location: "US",
// 	}

// 	dataset := client.Dataset(datasetID)

// 	_, metadataErr := dataset.Metadata(ctx)

// 	if e, ok := metadataErr.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
// 		if err := dataset.Create(ctx, metadata); err != nil {
// 			return "", err
// 		}
// 	}

// 	return datasetID, nil

// }
