import globus_sdk as globus

def manage_single_transfer(transfer_client, src, dest, src_location, dest_location):
    # Initiate Transfer using TransferClient
    task_data = globus.TransferData(source_endpoint=src, destination_endpoint=dest)
    task_data.add_item(src_location, dest_location)
    task_session = transfer_client.submit_transfer(task_data)
    task_id = task_session["task_id"]
    print(f"Submitted Transfer under Transfer ID: {task_id}")

    # Wait Until the Transfer is Complete (Time in Seconds)
    while not transfer_client.task_wait(task_id, timeout=43200, polling_interval=15):
        print(".", end="")
    print(f"\n Transfer {task_id} has completed the following transfers:")
    for info in transfer_client.task_successful_transfers(task_id):
        print(f"     {info['source_path']} ---> {info['destination_path']}")

    # TODO: Verify the Transfer Completed Correctly, Retry Loop if there are Failures
