"""
templates/base.py
─────────────────
Shared HTML shell used by every POS report email.

Colour theme: platform brand
  - Header bg:      #1A1A1A  (near-black)
  - Accent bar:     #E8470A  (accent orange)
  - Stat pill 1:    #E8470A  (orange)
  - Stat pill 2:    #2D2D2D  (dark grey)
  - Card bg:        #F7F9FC
  - Body bg:        #F0F2F5
  - Dark-mode & Outlook dark overrides included

Individual templates call `render()` and supply only their own `body_inner` HTML snippet.
"""


def render(
    *,
    title: str,
    badge: str,
    body_inner: str,
    run_date: str,
    run_ts: str,
    row_count: int,
    col_count: int,
    column_names: str,
    sheet_name: str,
    filename: str,
    company_name: str = "Platform Analytics",
) -> str:
    """
    Render the full HTML email document.

    Parameters
    ----------
    title        : Report title shown in the header and <title> tag.
    badge        : Short badge label (e.g. "MACH2 · CANCELLATIONS").
    body_inner   : HTML snippet — intro paragraph(s) + bullet list specific to this report.
    run_date     : Human-readable date  e.g. "08 Jun 2026".
    run_ts       : Full timestamp       e.g. "2026-06-08 11:57:41 UTC".
    row_count    : Number of data rows in the attached Excel file.
    col_count    : Number of columns in the attached Excel file.
    column_names : Comma-separated column names shown in the summary card.
    sheet_name   : Excel sheet/tab name shown in the summary card.
    filename     : Attachment filename shown in the attachment notice.
    company_name : Sender branding (default: Platform Analytics).
    """
    year = run_date[-4:] if len(run_date) >= 4 else "2026"

    if row_count > 0:
        # Normal case — data found, attachment included
        row_pill_color    = "#E8470A"
        row_pill_subcolor = "#FFD4C0"
        attachment_notice = f'''<table role="presentation" class="attachment-notice" cellpadding="0" cellspacing="0" border="0" width="100%"
               style="background-color:#FFF3EE;border-left:4px solid #E8470A;border-radius:0 6px 6px 0;margin-bottom:8px;">
          <tr>
            <td style="padding:14px 18px;">
              <p style="margin:0;font-size:13px;color:#2D2D2D;font-family:Arial,sans-serif;line-height:1.6;">
                &#128206;&nbsp;
                <strong style="color:#C0370A;">{filename}</strong>
                is attached to this email.<br />
                Open it with Microsoft Excel or Google Sheets.
              </p>
            </td>
          </tr>
        </table>'''
    else:
        # No data — neutral grey pill, no attachment, informational notice instead
        row_pill_color    = "#718096"
        row_pill_subcolor = "#CBD5E0"
        attachment_notice = '''<table role="presentation" class="attachment-notice" cellpadding="0" cellspacing="0" border="0" width="100%"
               style="background-color:#F7F9FC;border-left:4px solid #718096;border-radius:0 6px 6px 0;margin-bottom:8px;">
          <tr>
            <td style="padding:14px 18px;">
              <p style="margin:0;font-size:13px;color:#2D2D2D;font-family:Arial,sans-serif;line-height:1.6;">
                &#8505;&#65039;&nbsp;
                <strong>No records found</strong> for this run - there is no
                Excel file attached to this email.
              </p>
            </td>
          </tr>
        </table>'''

    return f"""<!DOCTYPE html>
<html lang="en" xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta http-equiv="X-UA-Compatible" content="IE=edge" />
  <meta name="color-scheme" content="light dark" />
  <meta name="supported-color-schemes" content="light dark" />
  <!--[if gte mso 9]>
  <xml>
    <o:OfficeDocumentSettings>
      <o:AllowPNG/>
      <o:PixelsPerInch>96</o:PixelsPerInch>
    </o:OfficeDocumentSettings>
  </xml>
  <![endif]-->
  <title>{title}</title>
  <style type="text/css">

    /* ── Reset ── */
    body, table, td, a {{ -webkit-text-size-adjust:100%; -ms-text-size-adjust:100%; }}
    table, td {{ mso-table-lspace:0pt; mso-table-rspace:0pt; border-collapse:collapse; }}
    img {{ -ms-interpolation-mode:bicubic; border:0; outline:none; text-decoration:none; display:block; }}
    body {{ margin:0 !important; padding:0 !important; }}

    /* ── Outlook dark mode overrides ── */
    [data-ogsc] .email-wrapper      {{ background-color:#1C1C1E !important; }}
    [data-ogsc] .email-container    {{ background-color:#2C2C2E !important; }}
    [data-ogsc] .header             {{ background-color:#1A1A1A !important; }}
    [data-ogsc] .accent-bar         {{ background:#E8470A !important; }}
    [data-ogsc] .greeting           {{ color:#F5F5F5 !important; }}
    [data-ogsc] .intro-text         {{ color:#C7C7CC !important; }}
    [data-ogsc] .summary-card       {{ background-color:#3A3A3C !important; border-color:#48484A !important; }}
    [data-ogsc] .summary-card-title {{ color:#FF9060 !important; }}
    [data-ogsc] .label              {{ color:#98989F !important; }}
    [data-ogsc] .value              {{ color:#F5F5F5 !important; }}
    [data-ogsc] .attachment-notice  {{ background-color:#3A3A3C !important; border-left-color:#E8470A !important; }}
    [data-ogsc] .attachment-notice p {{ color:#F5F5F5 !important; }}
    [data-ogsc] .footer             {{ background-color:#1A1A1A !important; border-top-color:#48484A !important; }}
    [data-ogsc] .footer p           {{ color:#98989F !important; }}
    [data-ogsc] .footer strong      {{ color:#E8470A !important; }}

    /* ── Responsive ── */
    @media only screen and (max-width:480px) {{
      .email-container {{ width:100% !important; }}
      .body-content    {{ padding:24px 20px !important; }}
      .header          {{ padding:20px !important; }}
      .footer          {{ padding:16px 20px !important; }}
    }}

    /* ── Native dark mode (Apple Mail, iOS Mail) ── */
    @media (prefers-color-scheme: dark) {{
      .email-wrapper      {{ background-color:#1C1C1E !important; }}
      .email-container    {{ background-color:#2C2C2E !important; }}
      .header             {{ background-color:#1A1A1A !important; }}
      .greeting           {{ color:#F5F5F5 !important; }}
      .intro-text         {{ color:#C7C7CC !important; }}
      .summary-card       {{ background-color:#3A3A3C !important; border-color:#48484A !important; }}
      .summary-card-title {{ color:#FF9060 !important; }}
      .label              {{ color:#98989F !important; }}
      .value              {{ color:#F5F5F5 !important; }}
      .attachment-notice  {{ background-color:#3A3A3C !important; border-left-color:#E8470A !important; }}
      .attachment-notice p {{ color:#F5F5F5 !important; }}
      .footer             {{ background-color:#1C1C1E !important; border-top-color:#48484A !important; }}
      .footer p           {{ color:#98989F !important; }}
    }}

  </style>
</head>
<body style="margin:0;padding:0;background-color:#F0F2F5;">

<!--[if mso | IE]>
<table role="presentation" border="0" cellpadding="0" cellspacing="0" width="100%" style="background-color:#F0F2F5;">
<tr><td>
<![endif]-->

<div class="email-wrapper" style="width:100%;background-color:#F0F2F5;padding:32px 0;">

  <table role="presentation" class="email-container" cellpadding="0" cellspacing="0" border="0" align="center"
         style="max-width:620px;width:100%;margin:0 auto;background-color:#FFFFFF;border-radius:8px;overflow:hidden;box-shadow:0 2px 16px rgba(0,0,0,0.10);">

    <!-- ══ HEADER ══ -->
    <tr>
      <td class="header" style="background-color:#1A1A1A;padding:22px 28px;">
        <p style="font-size:20px;font-weight:700;color:#FFFFFF;font-family:Arial,sans-serif;margin:0;letter-spacing:0.5px;">
          {company_name}
        </p>
        <p style="font-size:12px;color:#E8470A;font-family:Arial,sans-serif;margin:6px 0 0 0;
                  font-weight:700;text-transform:uppercase;letter-spacing:1.4px;">
          {badge}
        </p>
      </td>
    </tr>

    <!-- ══ ACCENT BAR ══ -->
    <tr>
      <td class="accent-bar" style="height:4px;font-size:0;line-height:0;background-color:#E8470A;">
        <!--[if mso | IE]>&nbsp;<![endif]-->
      </td>
    </tr>

    <!-- ══ BODY ══ -->
    <tr>
      <td class="body-content" style="padding:36px 40px 28px 40px;background-color:#FFFFFF;">

        <!-- Greeting -->
        <p class="greeting" style="font-size:16px;font-weight:600;color:#1A1A1A;font-family:Arial,sans-serif;margin:0 0 10px 0;">
          Hello,
        </p>

        <!-- Report-specific intro + bullets -->
        <div class="intro-text" style="font-size:14px;color:#4A5568;font-family:Arial,sans-serif;line-height:1.7;margin:0 0 28px 0;">
          {body_inner}
        </div>

        <!-- ══ Stat pills ══ -->
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom:28px;">
          <tr>
            <td width="50%" style="padding-right:6px;">
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                <tr>
                  <td style="background-color:{row_pill_color};border-radius:6px;padding:18px 12px;text-align:center;" bgcolor="{row_pill_color}">
                    <span style="font-size:28px;font-weight:700;color:#FFFFFF;font-family:Arial,sans-serif;display:block;line-height:1;mso-color-alt:#FFFFFF;">
                      {row_count:,}
                    </span>
                    <span style="font-size:10px;color:{row_pill_subcolor};font-family:Arial,sans-serif;text-transform:uppercase;letter-spacing:1px;margin-top:6px;display:block;mso-color-alt:{row_pill_subcolor};">
                      Total Rows
                    </span>
                  </td>
                </tr>
              </table>
            </td>
            <td width="50%" style="padding-left:6px;">
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                <tr>
                  <td style="background-color:#2D2D2D;border-radius:6px;padding:18px 12px;text-align:center;" bgcolor="#2D2D2D">
                    <span style="font-size:28px;font-weight:700;color:#FFFFFF;font-family:Arial,sans-serif;display:block;line-height:1;mso-color-alt:#FFFFFF;">
                      {col_count}
                    </span>
                    <span style="font-size:10px;color:#AAAAAA;font-family:Arial,sans-serif;text-transform:uppercase;letter-spacing:1px;margin-top:6px;display:block;mso-color-alt:#AAAAAA;">
                      Columns
                    </span>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <!-- ══ Summary card ══ -->
        <table role="presentation" class="summary-card" cellpadding="0" cellspacing="0" border="0" width="100%"
               style="background-color:#F7F9FC;border:1px solid #E2E8F0;border-radius:6px;margin-bottom:28px;">
          <tr>
            <td style="padding:20px 24px;">
              <p class="summary-card-title"
                 style="font-size:10px;font-weight:700;color:#E8470A;font-family:Arial,sans-serif;
                        text-transform:uppercase;letter-spacing:1.4px;margin:0 0 14px 0;">
                Report Summary
              </p>
              <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">

                <tr>
                  <td class="label" width="42%" style="font-size:13px;color:#718096;font-family:Arial,sans-serif;font-weight:500;padding:7px 0;">Report Name</td>
                  <td class="value"             style="font-size:13px;color:#1A1A1A;font-family:Arial,sans-serif;font-weight:600;padding:7px 0;">{title}</td>
                </tr>
                <tr><td colspan="2"><div style="height:1px;background-color:#E2E8F0;font-size:0;line-height:0;">&nbsp;</div></td></tr>

                <tr>
                  <td class="label" style="font-size:13px;color:#718096;font-family:Arial,sans-serif;font-weight:500;padding:7px 0;">Sheet Name</td>
                  <td class="value" style="font-size:13px;color:#1A1A1A;font-family:Arial,sans-serif;font-weight:600;padding:7px 0;">{sheet_name}</td>
                </tr>
                <tr><td colspan="2"><div style="height:1px;background-color:#E2E8F0;font-size:0;line-height:0;">&nbsp;</div></td></tr>

                <tr>
                  <td class="label" style="font-size:13px;color:#718096;font-family:Arial,sans-serif;font-weight:500;padding:7px 0;">Columns</td>
                  <td class="value" style="font-size:13px;color:#1A1A1A;font-family:Arial,sans-serif;font-weight:600;padding:7px 0;">{column_names}</td>
                </tr>
                <tr><td colspan="2"><div style="height:1px;background-color:#E2E8F0;font-size:0;line-height:0;">&nbsp;</div></td></tr>

                <tr>
                  <td class="label" style="font-size:13px;color:#718096;font-family:Arial,sans-serif;font-weight:500;padding:7px 0;">Generated At</td>
                  <td class="value" style="font-size:13px;color:#1A1A1A;font-family:Arial,sans-serif;font-weight:600;padding:7px 0;">{run_ts}</td>
                </tr>
                <tr><td colspan="2"><div style="height:1px;background-color:#E2E8F0;font-size:0;line-height:0;">&nbsp;</div></td></tr>

              </table>
            </td>
          </tr>
        </table>

        <!-- ══ Attachment / no-data notice ══ -->
        {attachment_notice}

      </td>
    </tr>

    <!-- ══ FOOTER ══ -->
    <tr>
      <td class="footer"
          style="background-color:#F7F9FC;border-top:1px solid #E2E8F0;padding:20px 40px;text-align:center;">
        <p style="margin:0;font-size:11px;color:#A0AEC0;font-family:Arial,sans-serif;line-height:1.8;">
          This is an automated report generated by
          <strong style="color:#E8470A;">{company_name}</strong>.
        </p>
        <p style="margin:8px 0 0 0;font-size:11px;font-weight:600;color:#718096;font-family:Arial,sans-serif;">
          {run_ts} &nbsp;|&nbsp; &copy; {year} Platform Analytics
        </p>
      </td>
    </tr>

  </table>
</div>

<!--[if mso | IE]>
</td></tr></table>
<![endif]-->

</body>
</html>"""