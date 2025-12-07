"""
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
"""

import enum
from typing import Dict, Optional, Any
from Deprecated.ErrorHandling import CoreException
from types import MappingProxyType
import inspect

from dataclasses import dataclass

@dataclass
class InjectableRecord:
    """
    A record of an injectable dependency.
    """
    instance: Any
    state: 'RState'


class AutoInject(type):

    """
    A metaclass made to resolve Injectable dependencies automatically.
    It uses a singleton-like Private Storage class to store and resolve dependencies from it.
    """

    _storage_instance: Optional['PrivateDependencyStorage'] = None
    _finalized = False
    @classmethod
    def get_instance(cls) -> Optional['PrivateDependencyStorage']:
        if cls._storage_instance is None:
            cls._storage_instance = PrivateDependencyStorage()
        return cls._storage_instance

    @classmethod
    def finalize(cls):
        """
        This method is called to finalize the dependency injection process.
        It prevents any further injections and resolves all dependencies.
        """
        if cls._finalized:
            raise DependencyInjectionError(
                "AutoInject.finalize",
                "Dependency Injection Error",
                "Dependency Injection is already finalized. No more dependencies can be injected.",
            )
        cls._finalized = True
        storage = cls.get_instance()
        storage.resolve_all_deps_after_init()

    def inspect_init_signature(cls):
        """
        This method inspects the __init__ method of the class and returns the parameters.
        """
        if cls._finalized:
            raise DependencyInjectionError(
                "AutoInject.__call__",
                "Dependency Injection Error",
                "Dependency Injection is already finalized. No more dependencies can be injected.",
            )

        # Get the signature of the __init__ method
        if hasattr(cls, '__init__'):
            signature = inspect.signature(cls.__init__)
            params_without_defaults = [
                name for name, param in signature.parameters.items()
                if param.default is param.empty and name != 'self'
            ]
            params_with_default = {
                name: param.default for name, param in signature.parameters.items()
                if param.default is not param.empty
            }
            return params_without_defaults, params_with_default
        else:
            raise ValueError(f"Constructor of {cls.__name__} is not defined. Please define __init__ method.")
    def __call__(cls, *args, **kwargs):
        """
        This method is called when the class is instantiated.
        It initializes the dependencies and resolves them if necessary.
        """
        storage: PrivateDependencyStorage = cls.get_instance()

        # Register the dependency first

        params_without_defaults, params_with_default = cls.inspect_init_signature()
        resolved = {}
        unresolved = []

        for param in params_without_defaults:
            if param == "self":
                continue
            if param in storage and param not in kwargs:
                try:
                    resolved[param] = storage.resolve(param).instance
                except DependencyInjectionError as e:
                    resolved[param] = None
                    unresolved.append(param)
            elif param in kwargs:
                resolved[param] = kwargs[param]
            else:
                resolved[param] = None


        for param, default_value in params_with_default.items():
            if param in storage:
                resolved[param] = storage.resolve(param)
            elif param not in kwargs:
                resolved[param] = default_value


        resolved.update(kwargs)
        instance = super().__call__(*args, **resolved)
        if instance.is_dependency:
            # Register the dependency in the storage
            storage.add(cls.__name__, instance)
            storage.enforce_injection_to_all_demanders(cls.__name__)
            if unresolved:
                storage.set_resolution(cls.__name__, RState.RESOLVING)
            else:
                storage.set_resolution(cls.__name__, RState.RESOLVED)
        return instance

class DependencyInjectionError(CoreException):
    destination = "default"

class Injectable(metaclass=AutoInject):
    is_dependency: bool = True



class RState(enum.Enum):
    """
    Enum to represent the state of a dependency resolution.
    """
    UNRESOLVED = 0
    RESOLVING = 1
    RESOLVED = 2

r_state_mapping = MappingProxyType({
    RState.UNRESOLVED: "UNRESOLVED",
    RState.RESOLVING: "RESOLVING",
    RState.RESOLVED: "RESOLVED",
    })

class SState(enum.Enum):
    """
    Enum to represent the state of a storage.
    """
    NO_TARGETS_INITIALIZED = 0
    NOT_ALL_TARGETS_INITIALIZED = 1
    ALL_TARGETS_INITIALIZED = 2


class PrivateDependencyStorage:

    """
    This class is responsible for storing and managing dependencies.
    It allows adding, resolving, and checking the status of dependencies.
    It also provides a way to resolve all dependencies after initialization.
    It is not ment to be used directly, but rather through the AutoInject metaclass at your Injectable.
    """

    def __init__(self):
        self.resolution_policy = None # Can be "lazy" or "eager"
        self._dependencies: Dict[str, InjectableRecord] = {}
        self.current_resolution_state = SState.NO_TARGETS_INITIALIZED

    def __contains__(self, item):
        return item in self._dependencies

    def add(self, name: str, dep_inst: Injectable):
        """
        Adds a dependency to the storage. May conflict if the type of dependency is already present in storage.
        Basic storage keeping policy is to keep all dependencies as singleton-like.
        """
        if name in self._dependencies:
            raise DependencyInjectionError(
                "Dependency already exists",
                f"Dependency with name {name} already exists in the storage.",
                "Dependency Injection Error"
            )
        self._dependencies[name] = InjectableRecord(instance=dep_inst, state=RState.UNRESOLVED)

    def resolve(self, name: str) -> InjectableRecord:
        """
        Resolves a dependency by its name.
        """
        if name not in self._dependencies:
            raise DependencyInjectionError(
                "Dependency not found",
                f"Dependency with name {name} not found in the storage.",
                ""
            )
        self._dependencies[name].state = RState.RESOLVING if self._dependencies[name].state == RState.UNRESOLVED else self._dependencies[name].state
        return self._dependencies[name] # note: this is a tuple and its essential to be tuple, because AutoInject will change state of it.



    def _force_inject(self, demander: Injectable):
        """
        Forces injection of dependencies into the demander.
        This method is used after the initialization of all dependencies.
        """
        if self.current_resolution_state != SState.ALL_TARGETS_INITIALIZED:
            raise DependencyInjectionError(
                where="PrivateDependencyStorage.force_inject",
                what="Not all dependencies are initialized",
                summary="You need to initialize all dependencies before forcing injection. If you initialized all dependencies, use finalize_dependencies() to indicate it."
            )

        for name, record in self._dependencies.items():
            dep_inst, state = record.instance, record.state
            if hasattr(demander, name) and getattr(demander, name) is None:
                try:
                    setattr(demander, name, dep_inst)
                except Exception as e:
                    print(e)

    def enforce_injection_to_all_demanders(self, name: str):
        """
        Enforces injection to all dependencies that demand this dependency.
        """
        if name not in self._dependencies:
            raise DependencyInjectionError(
                "Dependency not found",
                f"Dependency with name {name} not found in the storage.",
                ""
            )
        for dep_name, record in self._dependencies.items():
            dep_inst, state = record.instance, record.state
            if hasattr(dep_inst, name) and getattr(dep_inst, name) is None:
                setattr(dep_inst, name, self._dependencies[name].instance)


    def set_resolution(self, name, state: RState):
        """
        Sets the resolution state of a dependency.
        """
        if name not in self._dependencies:
            raise DependencyInjectionError(
                "Dependency not found",
                f"Dependency with name {name} not found in the storage.",
                ""
            )
        self._dependencies[name].state = state

    def resolve_all_deps_after_init(self):
        """
        Resolves all dependencies after initialization. Can resolve both cycle and not cycle deps.
        """
        if self.current_resolution_state != SState.ALL_TARGETS_INITIALIZED:
            raise DependencyInjectionError(
                where="PrivateDependencyStorage.resolve_all_deps_after_init",
                what="Not all dependencies are initialized",
                summary="You need to initialize all dependencies before resolving them. If you initialized all dependencies, use finalize_dependencies() to indicate it."
            )

        for name, record in self._dependencies.items():
            if record.state == RState.UNRESOLVED or record.state == RState.RESOLVING:
                try:
                    self._force_inject(record.instance)
                    record.state = RState.RESOLVED
                except Exception as e:
                    print(e)
                    record.state = RState.RESOLVING

    def __repr__(self):
        """
        Returns a string representation of the storage.
        """
        deps_str = ""
        for name, (dep_inst, state) in self._dependencies.items():
            deps_str += f"{name}: {r_state_mapping[state]}\n"



def finalize_dependencies():
    """
    Indicates initialization of a final dependency. That means after invoking this function, no auto-injection will be performed.
    """
    AutoInject.finalize()


