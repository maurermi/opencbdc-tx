# Add a new runner

# Choosing a smart contract environment

Different smart contract environments have different capabilities. For this example,
Python was chosen because of its C/C++ integration (through CPython), and its popularity in the development community.
Interpreted or bytecode based execution environments are easier to work with as well.

# Adding an extension

To add a new runner environment, it is reccomended to start by creating a new project in the parsec/agent/runners
subdirectory. At the very least, a server interface is required for the runner, and a basic TCP server
can be created based off of the Lua server. Create an implementation class to dictate behavior of the
system interacting with the runner.

# Updating state



# Future work

