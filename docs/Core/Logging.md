Logging Module
--------------
This module provides asynchronous logging functionality with support for log levels,
file rotation, and custom logging policies for the TELERAG-MONOLITH project.

Key Components:
1. Exceptions:
   - LoggingCreationException: Raised when there is an error during logger creation.
   - LoggingCancellation: Raised to signal cancellation of logging operations.

2. Logger Infrastructure:
   - BaseLogger: Abstract base class defining the logger interface.
   - Logger: Implements asynchronous logging, maintains a message queue, and writes logs to files.
   - LoggerComposer: Manages and registers logger instances, ensuring a singleton pattern for global access.
   - ComposerMeta: A metaclass that automates logger registration to the LoggerComposer.

3. Log Levels and Rotation:
   - LogLevel: Enumerates available log levels (DEBUG, INFO, WARNING, ERROR, etc.).
   - RotType: Defines the rotation type for log files (NONE, TIME, SIZE, TIME_SIZE).
   - LogPolicy: Specifies policies to handle failed flush attempts (PRINT, LOOSE, KEEP).
   - FileGateway: Handles file operations, message buffering, and log file rotation based on configured policies.

4. Utility Functions:
   - aprint and aprint_err: Asynchronous print functions to stdout and stderr respectively.
   - stop_logging: Shuts down all loggers and file gateways gracefully.

Usage:
- Instantiate a Logger (or a subclass) to start logging. The creation process automatically registers
  it via ComposerMeta.
- Configure logging levels and file rotation settings as needed.
- Use async methods like info, debug, error, etc., for logging messages.
- Call stop_logging() after the application completes its logging tasks to ensure proper shutdown.