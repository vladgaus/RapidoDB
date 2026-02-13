# Contributing to RapidoDB

Thank you for your interest in contributing to RapidoDB! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites
- Go 1.22 or higher
- Git
- Make (optional, but recommended)

### Getting Started

```bash
# Clone the repository
git clone https://github.com/vladgaus/RapidoDB.git
cd RapidoDB

# Run tests
make test

# Build
make build
```

## Development Workflow

### Branch Strategy

```
main (stable)
  └── develop (integration)
        ├── feature/step-2-skiplist
        ├── feature/step-3-wal
        └── fix/some-bug
```

1. **main**: Always stable, tagged releases
2. **develop**: Integration branch for next release
3. **feature/xxx**: Feature branches for each step

### Making Changes

1. Create a feature branch from `develop`:
   ```bash
   git checkout develop
   git pull origin develop
   git checkout -b feature/step-X-component-name
   ```

2. Make your changes, commit with clear messages:
   ```bash
   git add .
   git commit -m "feat(memtable): implement SkipList data structure"
   ```

3. Push and create a Pull Request to `develop`:
   ```bash
   git push origin feature/step-X-component-name
   ```

4. After review and merge to `develop`, prepare for release

---

## 🚀 Release Process

### Version Scheme

We follow [Semantic Versioning](https://semver.org/):

```
v0.STEP.PATCH

Examples:
- v0.1.0  → Step 1 (Project Scaffold)
- v0.1.1  → Step 1 bug fix
- v0.2.0  → Step 2 (SkipList MemTable)
- v0.15.0 → Step 15 (Benchmarks)
- v1.0.0  → First stable release
```

### Creating a Release

#### Option 1: Via Git Tags (Recommended)

```bash
# 1. Make sure you're on main and it's up to date
git checkout main
git pull origin main

# 2. Merge develop into main
git merge develop

# 3. Create and push a version tag
git tag -a v0.1.0 -m "Release v0.1.0: Project Scaffold"
git push origin main --tags
```

The GitHub Action will automatically:
- Run tests
- Build binaries for Linux, macOS, Windows (amd64/arm64)
- Create GitHub Release with changelog
- Upload archives and checksums

#### Option 2: Via GitHub UI

1. Go to **Releases** → **Draft a new release**
2. Click **Choose a tag** → Type `v0.1.0` → **Create new tag**
3. Set **Target** to `main`
4. Title: `RapidoDB v0.1.0`
5. Click **Generate release notes** or write manually
6. Click **Publish release**

### Release Checklist

Before releasing, ensure:

- [ ] All tests pass: `make test`
- [ ] Benchmarks run: `make bench`
- [ ] Code is formatted: `make fmt`
- [ ] No vet issues: `make vet`
- [ ] CHANGELOG.md is updated
- [ ] README.md is current
- [ ] Version in code matches tag (if applicable)

### Example: Releasing Step 1

```bash
# After Step 1 is complete and merged to main

# Create the release tag
git checkout main
git pull origin main
git tag -a v0.1.0 -m "Release v0.1.0: Project Scaffold

Features:
- Project structure with Go modules
- Configuration system (JSON-based)
- Core types: Entry, InternalKey, KeyRange
- Storage interfaces
- Binary encoding utilities
- Error handling
- Test infrastructure
- CI/CD with GitHub Actions"

# Push the tag to trigger release
git push origin v0.1.0
```

---

## Commit Message Convention

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `test`: Adding/updating tests
- `refactor`: Code refactoring
- `perf`: Performance improvement
- `chore`: Build, CI, dependencies

### Scopes
- `memtable`: MemTable related
- `wal`: Write-Ahead Log
- `sstable`: SSTable format
- `compaction`: Compaction strategies
- `bloom`: Bloom filters
- `lsm`: LSM engine core
- `mvcc`: MVCC/snapshots
- `server`: TCP server
- `config`: Configuration
- `encoding`: Binary encoding
- `health`: Health checks
- `graceful`: Graceful Shutdown

### Examples

```bash
feat(memtable): implement concurrent SkipList

- Add lock-free SkipList implementation
- Support concurrent reads and writes
- Add comprehensive benchmarks

Closes #12

---

fix(wal): handle fsync errors correctly

Previously, fsync errors were silently ignored which could
lead to data loss. Now we properly propagate errors.

---

test(sstable): add fuzz tests for reader

---

docs(readme): update installation instructions
```

---

## Code Style

- Run `go fmt` before committing
- Run `go vet` to check for issues
- Add comments for exported functions
- Write table-driven tests
- Include benchmarks for performance-critical code

## Questions?

Open an issue for any questions or discussions!
