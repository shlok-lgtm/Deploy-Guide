# MetaMask Snap — Grade Removal TODO

The MetaMask Snap source is in a separate repository. The following changes
are required to remove letter grade display:

1. **Score display**: Replace any letter grade (A/B/C/D/F) display with
   the numeric score (0-100). The score is the only output.

2. **Grade badges**: If the Snap renders colored badges based on letter
   grades, replace with score-based color coding:
   - Score >= 90: excellent (dark/black)
   - Score 70-89: good (medium)
   - Score 50-69: fair (light)
   - Score < 50: weak (red/accent)

3. **API response parsing**: The `grade` field is no longer returned by
   the Basis API. Remove any code that reads `response.grade` or
   `data.grade`. Use `response.score` / `data.score` instead.

4. **Tooltips/labels**: Any text that says "Grade: A+" should say
   "Score: 92.3" (or whatever the numeric value is).

5. **On-chain reads**: The oracle `getScore()` still returns a `bytes2 grade`
   field in its return tuple for ABI compatibility, but this field is no
   longer populated (set to 0x0000). Do not display it.

## Why

Using "rating" or letter grades risks triggering NRSRO/CRA classification
in the US and BMR administrator obligations in the EU. All Basis surfaces
use numerical scores only.
