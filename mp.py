import multiprocessing
import time

def process_item(item):
    """Simulate processing of a single item."""
    time.sleep(0.5)  # Simulate some work
    return f"Processed {item}"

def final_task(results):
    """Perform a task once all items are processed."""
    print("All tasks are complete!")
    print("Results:", results)

def main():
    items = list(range(20))  # Example list of items to process
    num_workers = multiprocessing.cpu_count()  # Use all available CPU cores

    with multiprocessing.Pool(num_workers) as pool:
        # Map items to the worker processes
        results = pool.map(process_item, items)

    # Perform a final task after all iterations are complete
    final_task(results)

if __name__ == "__main__":
    main()