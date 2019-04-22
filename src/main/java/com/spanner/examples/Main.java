package com.spanner.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.simba.cloudspanner.core.jdbc42.CloudSpanner42DataSource;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {


    public static void main(String[] args) {
        DataSource dataSource = get();
        try {
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            String PUSH_MESSAGE_INSERT = "INSERT INTO queue_message (id, created_on, deliver_on, queue_name, message_id, popped, offset_time_seconds, payload) " +
                    "VALUES (1 , " + timestamp + "," + timestamp + ", \"test\", 1234,false,3, \"test\")";
            System.out.println(PUSH_MESSAGE_INSERT);
            PreparedStatement preparedStatement = connection.prepareStatement(PUSH_MESSAGE_INSERT);
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException ex) {
            System.out.println("sql exception : " + ex.getMessage());
        }


        ProjectTopicName topicName = ProjectTopicName.of("gcp-ftd-nonprod-gke", "dev3.conductor.esupdate");
        Publisher publisher = null;

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).setCredentialsProvider(getCredential()).build();

            List<String> messages = Arrays.asList("first message", "second message");

            for (final String message : messages) {
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                // Once published, returns a server-assigned message id (unique within the topic)
                ApiFuture<String> future = publisher.publish(pubsubMessage);

                // Add an asynchronous callback to handle success / failure
                ApiFutures.addCallback(
                        future,
                        new ApiFutureCallback<String>() {

                            @Override
                            public void onFailure(Throwable throwable) {
                                if (throwable instanceof ApiException) {
                                    ApiException apiException = ((ApiException) throwable);
                                    // details on the API exception
                                    System.out.println(apiException.getStatusCode().getCode());
                                    System.out.println(apiException.isRetryable());
                                }
                                System.out.println("Error publishing message : " + message);
                            }

                            @Override
                            public void onSuccess(String messageId) {
                                // Once published, returns server-assigned message ids (unique within the topic)
                                System.out.println(messageId);
                            }
                        },
                        MoreExecutors.directExecutor());
            }
        } catch (Exception ex) {
            ex.printStackTrace();

        } finally {
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                try {
                    publisher.shutdown();
                    publisher.awaitTermination(1, TimeUnit.MINUTES);
                } catch (Exception ex) {

                }

            }
        }


    }

    protected static String toJson(Object value) {
        ObjectMapper om = new ObjectMapper();
        if (null == value) {
            return null;
        }

        try {
            return om.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            System.out.println("Error occurred while converting the object " + ex.getMessage());
        }
        return null;
    }

    public static CredentialsProvider getCredential() {
        String pubSubCredentialScopePropertyValue = "https://www.googleapis.com/auth/pubsub";
        String[] pubSubCredentialScopes = pubSubCredentialScopePropertyValue.split(",");

        List<String> credentials = Collections.unmodifiableList(Arrays.asList(pubSubCredentialScopes));
        CredentialsProvider credentialsProvider = null;

        try {
            credentialsProvider =
                    FixedCredentialsProvider.create(GoogleCredentials.fromStream(
                            new FileInputStream(new File("C:\\Users\\gbreddy\\Desktop\\gcp-ftd-nonprod-gke.json"))).createScoped(credentials));
        } catch (IOException e) {

        }


        return credentialsProvider;
    }


    public static DataSource get() {

        CloudSpanner42DataSource dataSource = new CloudSpanner42DataSource();

        dataSource.setURL("jdbc:cloudspanner://localhost;Project=gcp-ftd-nonprod-gke;Instance=test;" +
                "Database=first;SimulateProductName=MySQL;" +
                "PvtKeyPath=;C:\\Users\\gbreddy\\Desktop\\gcp-ftd-nonprod-gke.json;" +
                "AllowExtendedMode=false");

        return dataSource;
    }


}
