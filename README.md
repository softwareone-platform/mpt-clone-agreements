# Marketplace Platform Agreement Clone Scripts

Simple scripts to clone Marketplace Platform agreements by changing either the **licensee** or **listing** while keeping all other agreement details intact.

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables. You can either:

   **Option A:** Put them in `~/.mpt-clone-agreement` file:
   ```
   OPS_TOKEN=your_operations_token
   VENDOR_TOKEN=your_vendor_token
   API_URL=https://portal.s1.live
   ```

   **Option B:** Export them as environment variables:
   ```bash
   export OPS_TOKEN=your_operations_token
   export VENDOR_TOKEN=your_vendor_token
   export API_URL=https://portal.s1.live
   ```

Optional variables (only needed for `--microsoft-sync` flag):
- `CSP_URL_TUNNEL` - CSP tunnel URL
- `CSP_TOKEN` - CSP token

## Usage

### 1. Dump Agreement (`dump_agreement.py`)

Extract agreement and subscription data. Choose either `--listing-id` or `--licensee-id`:

```bash
# Clone to a different listing
python dump_agreement.py --agreement-id AGR-1234-5678-9012 --listing-id LST-9279-6638

# Clone to a different licensee
python dump_agreement.py --agreement-id AGR-1234-5678-9012 --licensee-id LCE-1234-5678-9012
```

Output files are saved in `output/AGR-XXXX-XXXX-XXXX/`:
- `agreement_object.json` - Original agreement
- `new_agreement_object.json` - Modified agreement ready for creation
- `subscriptions.xlsx` - Subscription data
- `authorization.json` - Authorization details
- `SUB-XXXX-XXXX-XXXX.json` - Individual subscription JSON files

### 2. Create New Agreement (`create_new_agreement.py`)

Create the new agreement from the dumped data:

```bash
# Create subscriptions from Excel/JSON files
python create_new_agreement.py --agreement-id AGR-1234-5678-9012

# Or trigger Microsoft platform sync
python create_new_agreement.py --agreement-id AGR-1234-5678-9012 --microsoft-sync
```

### 3. Update Subscription Markups (`update_subscription_markups.py`)

Update markups for subscriptions based on Excel data:

```bash
# Dry run (default)
python update_subscription_markups.py --agreement-id AGR-1234-5678-9012

# Apply changes
python update_subscription_markups.py --agreement-id AGR-1234-5678-9012 --no-dry-run

# Keep purchase price when updating
python update_subscription_markups.py --agreement-id AGR-1234-5678-9012 --no-dry-run --keep-purchase-price
```

### 4. Terminate Agreement (`terminate_agreement.py`)

Terminate all subscriptions for an agreement:

```bash
python terminate_agreement.py --agreement-id AGR-1234-5678-9012
```

## Requirements

- **Operations Token (OPS_TOKEN)**: Required for reading agreements and subscriptions
- **Vendor Token (VENDOR_TOKEN)**: Required for creating/updating agreements and subscriptions
- **CSP Token (CSP_TOKEN)**: Only required if using `--microsoft-sync` flag

## Workflow

1. Run `dump_agreement.py` with either `--listing-id` or `--licensee-id`
2. Review the generated files in `output/AGR-XXXX-XXXX-XXXX/`
3. Run `create_new_agreement.py` to create the new agreement
4. (Optional) Run `update_subscription_markups.py` to adjust pricing
5. (Optional) Run `terminate_agreement.py` to terminate the original agreement

## License

This project is licensed under the Apache License 2.0. See the LICENSE file for details.
