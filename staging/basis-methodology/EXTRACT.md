# Extracting `basis-methodology` to its own repo

This tree lives under `basis-hub/staging/basis-methodology/` as a stop-gap
because the automated toolchain that created it cannot provision a new
GitHub repository. Once a human operator is available, extract it:

```bash
# 1. From a fresh clone of basis-hub:
cd /tmp
git clone https://github.com/basis-protocol/basis-hub.git
cd basis-hub

# 2. Split staging/basis-methodology out into its own history:
git subtree split --prefix=staging/basis-methodology -b methodology-extracted

# 3. Create the new repo on GitHub (basis-protocol/basis-methodology) — empty.
# 4. Push the split branch:
git remote add methodology git@github.com:basis-protocol/basis-methodology.git
git push methodology methodology-extracted:main

# 5. In basis-hub, remove the staging tree and commit:
cd /path/to/basis-hub
git rm -r staging/basis-methodology
git commit -m "chore: extract basis-methodology to its own repo"
git push origin main
```

After extraction:

- The `.github/workflows/reproducibility.yml` workflow activates in the
  new repo.
- Update `basis-hub/README.md` "See also" section to link to the new
  repo instead of `staging/basis-methodology/`.
- Update `docs/basis_protocol_v9_3_constitution_amendment.md` Article II
  references.
