extern crate ask_cqrs;

use std::error::Error;

use eventstore::{
    Client, EventData, PersistentSubscriptionToAllOptions, RetryOptions, StreamPosition,
    SubscribeToAllOptions, SubscriptionFilter,
};
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};

#[derive(Serialize, Deserialize, Debug)]
struct Foo {
    is_rust_a_nice_language: bool,
    more_data: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Creates a client settings for a single node configuration.
    // let settings = "esdb://admin:changeit@localhost:2113".parse()?;
    let settings =
        "esdb://127.0.0.1:2113?tls=false&keepAliveTimeout=10000&keepAliveInterval=10000".parse()?;

    let client = Client::new(settings)?;

    // It is not mandatory to use JSON as a data format however EventStoreDB
    // provides great additional value if you do so.
    let client_clone = client.clone();
    let client_clone2 = client.clone();
    let client_clone3 = client.clone();
    create_persistent_subscription(client_clone2).await.unwrap();

    tokio::spawn(async move {
        subscribe_to_all(client_clone).await.unwrap();
    });

    tokio::spawn(async move {
        listen_to_persistent_subscription(client_clone3)
            .await
            .unwrap();
    });

    let mut counter = 0;

    // let mut stream = client
    //     .read_stream("language-stream", &Default::default())
    //     .await?;

    // while let Some(event) = stream.next().await? {
    //     counter = counter + 1;
    // }

    loop {
        let payload = Foo {
            more_data: Some("event number is: ".to_string() + &counter.to_string()),
            is_rust_a_nice_language: true,
        };
        let evt = EventData::json("language-poll", &payload)?;
        println!("Appending to stream");
        client
            .clone()
            .append_to_stream("language-stream", &Default::default(), evt)
            .await?;

        counter = counter + 1;
        sleep(Duration::from_secs(5)).await;
    }

    // let mut stream = client
    //     .read_stream("language-stream", &Default::default())
    //     .await?;

    // while let Some(event) = stream.next().await? {kkk
    //     let event = event.get_original_event().as_json::<Foo>()?;

    //     // Do something productive with the result.
    //     println!("{:?}", event);
    // }

    Ok(())
}

pub async fn subscribe_to_all(client: Client) -> Result<(), Box<dyn Error>> {
    // region subscribe-to-all
    let filter = SubscriptionFilter::on_stream_name().add_prefix("language-");
    let options = SubscribeToAllOptions::default().filter(filter);
    let mut stream = client.subscribe_to_all(&options).await;

    loop {
        let event = stream.next().await?;
        println!("{:?}", event);
        let parsed_event = event.get_original_event().as_json::<Foo>();
        match parsed_event {
            Ok(f) => println!("{:?}", f.more_data),
            Err(e) => (),
        };
        // Handles the event...
    }

    Ok(())
}

pub async fn subscribe_to_stream(client: Client) -> Result<(), Box<dyn Error>> {
    // region subscribe-to-all
    let mut stream = client
        .subscribe_to_stream("$streams", &Default::default())
        .await;

    loop {
        let event = stream.next().await?;
        println!("{:?}", event);
        let parsed_event = event.get_original_event().as_json::<Foo>();
        match parsed_event {
            Ok(f) => println!("{:?}", f.more_data),
            Err(e) => (),
        };
        // Handles the event...
    }

    Ok(())
}

async fn create_persistent_subscription(client: Client) -> eventstore::Result<()> {
    // #region create-persistent-subscription-to-stream
    let options = PersistentSubscriptionToAllOptions::default()
    .filter(SubscriptionFilter::on_stream_name().add_prefix("language-"));

// 
    match client
        .create_persistent_subscription_to_all("subscription-group_tres", &options)
        .await
    {
        Ok(_) => Ok(()),
        Err(eventstore::Error::ResourceAlreadyExists) => Ok(()),
        Err(e) => Err(e),
    }
}

async fn listen_to_persistent_subscription(client: Client) -> eventstore::Result<()> {
    println!("listening to persistent subscription");
    let mut sub = client
        .subscribe_to_persistent_subscription_to_all("subscription-group_tres", &Default::default())
        .await?;

    loop {
        println!("waiting for event");
        let event = sub.next().await?;
        // Doing some productive work with the event...
        let parsed_event = event.get_original_event().as_json::<Foo>();
        match parsed_event {
            Ok(f) => println!(" persistent read: {:?}", f.more_data),
            Err(e) => println!("peristent error reading event"),
        };
        // Handles the event...

        sub.ack(event).await?;
    }
}
