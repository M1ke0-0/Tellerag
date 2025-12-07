Dependency Injection Module
-----------------------------
This module implements an automatic dependency injection system. It is built around the concept of a 
central dependency storage and the AutoInject metaclass. The key components are:

1. InjectableRecord:
   - A dataclass that holds an instance intended for injection along with its resolution state.

2. AutoInject (metaclass):
   - Intercepts instantiation of classes that inherit from Injectable.
   - Automatically inspects the __init__ signature, resolves dependencies from a central storage 
     (PrivateDependencyStorage), and injects them into the class instance.
   - Finalization through the finalize() method prevents further modification of dependencies.

3. DependencyInjectionError:
   - A custom exception handling dependency injection and resolution errors.

4. Injectable:
   - Base class for dependencies. Inheriting from this marks a class for automatic dependency management.

5. RState and SState:
   - Enumerations for tracking the resolution state of each dependency (e.g., UNRESOLVED, RESOLVING, RESOLVED)
     and the overall state of the dependency storage.

6. PrivateDependencyStorage:
   - A singleton-like storage class that manages the registration and resolution of dependencies.
   - Methods include adding dependencies, resolving them, forced injection, and batch resolution of all dependencies.

7. finalize_dependencies:
   - A helper function which calls AutoInject.finalize() to lock the dependency graph such that no 
     further injections can occur after all dependencies are initialized.

Usage:
- Mark a class as injectable by inheriting from Injectable.
- The AutoInject metaclass then intercepts the instantiation to automatically inject dependencies
  based on constructor parameters.
- Call finalize_dependencies() when all dependencies are initialized to enforce the final state.