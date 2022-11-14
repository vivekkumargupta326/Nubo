package com.amazonaws.samples;
/*
 * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.BufferedReader;
import java.util.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Sample {
	static Scanner sc;
	static AmazonS3 s3;
	static HashMap<String,String> bucket_to_id;
	//static HashMap<String,UUID> key_to_id;
	private static void displayFile()throws IOException
	{
		/*
         * Download an object - When you download an object, you get all of
         * the object's metadata and a stream from which to read the contents.
         * It's important to read the contents of the stream as quickly as
         * possibly since the data is streamed directly from Amazon S3 and your
         * network connection will remain open until you read all the data or
         * close the input stream.
         *
         * GetObjectRequest also supports several other options, including
         * conditional downloading of objects based on modification times,
         * ETags, and selectively downloading a range of an object.
         */
		System.out.println("Enter the bucket name : ");
		String bucket_name = sc.next();
		
		int status = listObjects(bucket_name);
		
		if(status == -1)
		{
			System.out.println("Bucket is empty\n");
			return;
		}
		if(status == -2)
		{
			System.out.println("Internal error, please read the error details\n");
			return;
		}
		bucket_name += bucket_to_id.get(bucket_name);
		
		System.out.println("Enter the key of the object you want to display : ");
		String key = sc.next();
        System.out.println("Downloading object "+key);
        S3Object object = s3.getObject(new GetObjectRequest(bucket_name, key));
        System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
        displayTextInputStream(object.getObjectContent());
	}
	private static int listObjects(String bucket_name)
	{
		try
		{
			/*
             * List objects in your bucket by prefix - There are many options for
             * listing the objects in your bucket.  Keep in mind that buckets with
             * many objects might truncate their results when listing their objects,
             * so be sure to check if the returned object listing is truncated, and
             * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
             * additional results.
             */
//			System.out.println("Enter the bucket name : ");
//			String bucket_name = sc.next();
			System.out.println("Listing objects in "+bucket_name);
			bucket_name += bucket_to_id.get(bucket_name);
         
            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                    .withBucketName(bucket_name));
            if(objectListing.getObjectSummaries().size() == 0)
            {
            	//System.out.println("Bucket is empty\n");
            	return -1; // for empty bucket
            }
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" -Object key =  " + objectSummary.getKey() + "  " +
                                   "(size = " + objectSummary.getSize() + ")");
            }
            System.out.println();
            return 1; //for successful execution
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            return -2; // for internal error
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
            return -2; // for internal error
        }
		//return false;
	}
	private static void listObjects()
	{
		try
		{
			/*
             * List objects in your bucket by prefix - There are many options for
             * listing the objects in your bucket.  Keep in mind that buckets with
             * many objects might truncate their results when listing their objects,
             * so be sure to check if the returned object listing is truncated, and
             * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
             * additional results.
             */
			System.out.println("Enter the bucket name : ");
			String bucket_name = sc.next();
			System.out.println("Listing objects in "+bucket_name);
			bucket_name += bucket_to_id.get(bucket_name);
         
            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                    .withBucketName(bucket_name));
            if(objectListing.getObjectSummaries().size() == 0)
            {
            	System.out.println("Bucket is empty\n");
            	return; // for empty bucket
            }
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                System.out.println(" -Object key =  " + objectSummary.getKey() + "  " +
                                   "(size = " + objectSummary.getSize() + ")");
            }
            System.out.println();
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            System.out.println("Some error occured\n");
            //return -2 ; // for internal error
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
            //return -2; // for internal error
        }
	}
	private static void deleteFile()
	{
		/*
         * Upload an object to your bucket - You can easily upload a file to
         * S3, or upload directly an InputStream if you know the length of
         * the data in the stream. You can also specify your own metadata
         * when uploading to S3, which allows you set a variety of options
         * like content-type and content-encoding, plus additional metadata
         * specific to your applications.
         */
		try
		{
			/*
            * Delete an object - Unless versioning has been turned on for your bucket,
            * there is no way to undelete an object, so use caution when deleting objects.
            */
			System.out.println("Enter the name of the bucket you want to delete a file from : ");
			String bucket_name = sc.next();
			
			int status = listObjects(bucket_name);
			
			if(status == -1)
			{
				System.out.println("Bucket is empty\n");
				return;
			}
			if(status == -2)
			{
				System.out.println("Internal error, please read the error details\n");
				return;
			}
			
			bucket_name += bucket_to_id.get(bucket_name);
			
			System.out.println("Enter the key of the object you want to delete from the above list : ");
			String key = sc.next();
            System.out.println("Deleting object "+key+"\n");
            s3.deleteObject(bucket_name, key);
            System.out.println("Object deleted successfully\n");
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
		
	}
	private static void listBuckets()
	{
		try
		{
			/*
             * List the buckets in your account
             */
            System.out.println("Listing buckets");
            if(s3.listBuckets().size() == 0)
            {
            	System.out.println("No Buckets to display at the moment ! ");
            	return;
            }
            for (Bucket bucket : s3.listBuckets()) {
                System.out.println(" - " + bucket.getName());
            }
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
	}
	private static void uploadFile()
	{
		/*
         * Upload an object to your bucket - You can easily upload a file to
         * S3, or upload directly an InputStream if you know the length of
         * the data in the stream. You can also specify your own metadata
         * when uploading to S3, which allows you set a variety of options
         * like content-type and content-encoding, plus additional metadata
         * specific to your applications.
         */
		try
		{
			System.out.println("Enter the name of the bucket you want to upload a file to : ");
			String bucket_name = sc.next();
			
			int status = listObjects(bucket_name);
			
			if(status == -2)
			{
				System.out.println("Internal error, please read the error details\n");
				return;
			}
			
			if(status == -1)
			{
				System.out.println("Bucket is empty\n");
			}
			
			
			bucket_name += bucket_to_id.get(bucket_name);
			System.out.println("Enter the key of the object ( make sure the key is unique within this bucket ) : ");
			String key = sc.next();
			System.out.println("Enter the location of the file : ");
			String location = sc.next();
	        System.out.println("Uploading a new object to S3 from a file\n");
	        s3.putObject(new PutObjectRequest(bucket_name, key, new File(location)));
	        System.out.println("Object uploaded successfully\n");
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
		
	}
	private static void deleteBucket()
	{
		try
		{
			System.out.println("Enter the name of the bucket you want to delete");
			String bucket_name = sc.next();
			bucket_name += bucket_to_id.get(bucket_name);
			/*
//	       * Delete a bucket - A bucket must be completely empty before it can be
//	       * deleted, so remember to delete any objects from your buckets before
//	       * you try to delete them.
////	       */
		     System.out.println("Deleting bucket " + bucket_name + "\n");
		     s3.deleteBucket(bucket_name);
		     System.out.println("Bucket deleted successfully");
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
		
	      
	}
	private static void createBucket()
	{
		try {
            /*
             * Create a new S3 bucket - Amazon S3 bucket names are globally unique,
             * so once a bucket name has been taken by any user, you can't create
             * another bucket with that same name.
             *
             * You can optionally specify a location for your bucket if you want to
             * keep your data closer to your applications or users.
             */
			System.out.println("Enter the Bucket name : ");
			String bucket_name = sc.next();
			UUID id =  UUID.randomUUID();
			String bucket_id = "-"+id;
			bucket_to_id.put(bucket_name,bucket_id);
			System.out.println("Creating bucket " + bucket_name + "\n");
			bucket_name+=bucket_id;
            s3.createBucket(bucket_name);
            System.out.println("Bucket created successfully");
            /*
             * List the buckets in your account
             */
//            System.out.println("Listing buckets");
//            for (Bucket bucket : s3.listBuckets()) {
//                System.out.println(" - " + bucket.getName());
//            }
            System.out.println();

            /*
             * Upload an object to your bucket - You can easily upload a file to
             * S3, or upload directly an InputStream if you know the length of
             * the data in the stream. You can also specify your own metadata
             * when uploading to S3, which allows you set a variety of options
             * like content-type and content-encoding, plus additional metadata
             * specific to your applications.
             */
//            System.out.println("Uploading a new object to S3 from a file\n");
//            s3.putObject(new PutObjectRequest(bucket_name, key, createSampleFile()));

            /*
             * Download an object - When you download an object, you get all of
             * the object's metadata and a stream from which to read the contents.
             * It's important to read the contents of the stream as quickly as
             * possibly since the data is streamed directly from Amazon S3 and your
             * network connection will remain open until you read all the data or
             * close the input stream.
             *
             * GetObjectRequest also supports several other options, including
             * conditional downloading of objects based on modification times,
             * ETags, and selectively downloading a range of an object.
             */
//            System.out.println("Downloading an object");
//            S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
//            System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
//            displayTextInputStream(object.getObjectContent());

            /*
             * List objects in your bucket by prefix - There are many options for
             * listing the objects in your bucket.  Keep in mind that buckets with
             * many objects might truncate their results when listing their objects,
             * so be sure to check if the returned object listing is truncated, and
             * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
             * additional results.
             */
//            System.out.println("Listing objects");
//            ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
//                    .withBucketName(bucketName)
//                    .withPrefix("My"));
//            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
//                System.out.println(" - " + objectSummary.getKey() + "  " +
//                                   "(size = " + objectSummary.getSize() + ")");
//            }
//            System.out.println();

            /*
             * Delete an object - Unless versioning has been turned on for your bucket,
             * there is no way to undelete an object, so use caution when deleting objects.
             */
//            System.out.println("Deleting an object\n");
//            s3.deleteObject(bucketName, key);
//
//            /*
//             * Delete a bucket - A bucket must be completely empty before it can be
//             * deleted, so remember to delete any objects from your buckets before
//             * you try to delete them.
////             */
//            System.out.println("Deleting bucket " + bucketName + "\n");
//            s3.deleteBucket(bucketName);
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
		
	}
    public static void main(String[] args) throws IOException {

        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\vivek\\.aws\\credentials).
         */
    	sc=new Scanner(System.in);
    	bucket_to_id = new HashMap<>();
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\vivek\\.aws\\credentials), and is in valid format.",
                    e);
        }

         s3= AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion("us-west-2")
            .build();

//        String bucketName = "my-first-s3-bucket-" + UUID.randomUUID();
//        String key = "MyObjectKey";

         
        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon S3");
        System.out.println("===========================================\n");
        boolean flag=false;
        while(!flag)
        {
        	System.out.println("Enter:\n 1 to create a bucket\n 2 to upload object to an existing bucket\n 3 to delete object from an existing bucket\n 4 to list objects in a bucket\n 5 to delete a bucket\n 6 to list buckets\n 7 to display the content of a file\n 8 to exit");
        	char choice = sc.next().charAt(0);
        	switch(choice)
        	{
        		case '1':
        			createBucket();
        			break;
        		case '2':
        			uploadFile(); 
        			break;
        		case '3':
        			deleteFile();
        			break;
        		case '4':
        			listObjects();
        			break;
        		case '5':
        			deleteBucket();
        			break;
        		case '6':
        			listBuckets();
        			break;
        		case '7':
        			displayFile();
        			break;
        		case '8':
        			System.out.println("Exiting..");
        			flag=true;
        			break;
        	}
        }
        
    }

    /**
     * Creates a temporary file with text data to demonstrate uploading a file
     * to Amazon S3
     *
     * @return A newly created temporary file with text data.
     *
     * @throws IOException
     */
    private static File createSampleFile() throws IOException {
        File file = File.createTempFile("aws-java-sdk-", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.close();

        return file;
    }

    /**
     * Displays the contents of the specified input stream as text.
     *
     * @param input
     *            The input stream to display as text.
     *
     * @throws IOException
     */
    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

}
