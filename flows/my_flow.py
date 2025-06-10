from prefect import flow, task

@task
def say_hello(name: str) -> str:
    print(f"Hello, {name}!")

@flow
def hello_flow(name: str = "World") -> None:
    greeting = say_hello(name)
    print(greeting)

if __name__ == "__main__":
    hello_flow("Prefect")