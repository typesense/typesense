# Typesense Benchmark CLI

A command-line tool for benchmarking, testing, and managing Typesense server installations. This tool provides utilities for installing specific versions of Typesense, running integration tests, and performing various benchmark operations.

## Features

- **Version Management**: Install and manage different versions of Typesense server
- **Docker Integration**: Automated Docker container management for consistent testing environments
- **Integration Testing**: Comprehensive test suite for Typesense features including:
  - Cluster operations
  - Conversation model testing
  - OpenAI embedding integration
  - Snapshot functionality
- **Cross-Platform Support**: Works on multiple architectures (amd64, arm64)
- **Configurable Environment**: Flexible configuration options for testing different scenarios

## Prerequisites

- Node.js (ES2023 or later)
- Docker
- pnpm (recommended) or npm
- Git

## Installation

1. Clone the repository:

```bash
git clone [repository-url]
cd benchmark
```

2. Install dependencies:

```bash
pnpm install
```

3. Build the project:

```bash
pnpm build
```

## Usage

The tool provides two main commands: `install` and `test`.

### Installing Typesense

```bash
typesense-benchmark install [options]

Options:
  -n, --container-name <name>     Name for the Docker container (default: "bazel-build")
  -i, --image-name <image>        Name for the Docker image (default: "ubuntu-build")
  -g, --typesense-git-url <url>   Git URL for the Typesense repo
  -d, --working-directory <dir>   Working directory for installation
  -c, --commitHash <hash>         Specific commit to install
  -y, --yes                       Answer yes to all prompts
  -v, --verbose                   Enable verbose output
```

### Running Tests

```bash
typesense-benchmark test [options]

Options:
  -n, --container-name <name>     Name for the Docker container
  -i, --image-name <image>        Name for the Docker image
  -g, --typesense-git-url <url>   Git URL for the Typesense repo
  -d, --working-directory <dir>   Working directory for tests
  -c, --commitHash <hash>         Specific commit to test
  -b, --binary <path>            Path to pre-built binary
  --api-key <key>                API key for Typesense (default: "xyz")
  --openAI-key <key>             OpenAI API key
  --ip <ip>                      IP address for Typesense
  -s, --snapshot-path <path>     Path for snapshot files
  -v, --verbose                  Enable verbose output
  -y, --yes                      Answer yes to all prompts
```

## Configuration

The tool can be configured through command-line options or environment variables:

- `OPENAI_API_KEY`: Your OpenAI API key for embedding tests

Core configuration files:

- `tsconfig.json`: TypeScript configuration
- `tsup.config.ts`: Build configuration
- `eslint.config.mjs`: Linting rules

## Development

### Project Structure

```
benchmark/
├── src/
│   ├── commands/     # CLI commands
│   ├── services/     # Core services
│   └── utils/        # Utility functions
├── test/            # Test files
└── dist/            # Compiled output
```

### Running Tests

```bash
# Run all tests
pnpm test
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please ensure that:

- Tests are added for new functionality
- Code follows the existing style conventions
- All tests pass
- Documentation is updated

## Credits

Created and maintained by Typesense, Inc. (contact@typesense.org)

Contributors:

- [Fanis Tharropoulos](https://github.com/tharropoulos) (fanis@typesense.org)
