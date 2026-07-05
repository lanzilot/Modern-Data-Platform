class DataValidator:

    @staticmethod
    def validate_dataframe(df, table_name, primary_key):
        issues = []

        if df.empty:
            return True, ["No new records found"]

        if primary_key.upper() in df.columns:
            pk = primary_key.upper()
        elif primary_key.lower() in df.columns:
            pk = primary_key.lower()
        else:
            issues.append(f"Primary key column {primary_key} not found")
            return False, issues

        duplicate_count = df.duplicated(subset=[pk]).sum()
        if duplicate_count > 0:
            issues.append(f"{duplicate_count} duplicate primary keys found in {table_name}")

        null_pk_count = df[pk].isnull().sum()
        if null_pk_count > 0:
            issues.append(f"{null_pk_count} null primary keys found in {table_name}")

        status = len(issues) == 0

        if status:
            issues.append("Validation passed")

        return status, issues