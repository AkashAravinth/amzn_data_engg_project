def lambda_handler(event, context):
    columns = [c['Label'] for c in event['ColumnMetadata']]
    records = event['Records']

    def extract_value(field):
        for key in ('StringValue', 'LongValue', 'DoubleValue', 'BooleanValue'):
            if key in field:
                return field[key]
        return ''

    rows = [[extract_value(field) for field in record] for record in records]

    header_html = ''.join(f'<th style="border:1px solid #ddd;padding:8px;background:#f4f4f4;">{col}</th>' for col in columns)
    rows_html = ''
    for row in rows:
        cells = ''.join(f'<td style="border:1px solid #ddd;padding:8px;">{val}</td>' for val in row)
        rows_html += f'<tr>{cells}</tr>'

    html = f"""
    <html><body>
        <h2>Data Engineering Project Report</h2>
        <table style="border-collapse:collapse;width:100%;">
            <tr>{header_html}</tr>
            {rows_html}
        </table>
    </body></html>
    """

    return {"htmlBody": html, "rowCount": len(rows)}