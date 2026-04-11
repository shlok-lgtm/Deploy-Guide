import { logger } from "./logger.js";

export async function sendAlert(message: string, error?: unknown): Promise<void> {
  const errorStr = error instanceof Error
    ? error.message
    : error != null
    ? String(error)
    : undefined;

  const fullMessage = `${message}${errorStr ? `\n${errorStr}` : ""}`;
  logger.error("ALERT: " + message, errorStr ? { error: errorStr } : undefined);

  // Email via Resend
  const resendKey = process.env["RESEND_API_KEY"];
  if (resendKey) {
    const alertEmail = process.env["ALERT_EMAIL"] ?? "shlok@basisprotocol.xyz";
    try {
      const res = await fetch("https://api.resend.com/emails", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${resendKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          from: "alerts@basisprotocol.xyz",
          to: [alertEmail],
          subject: "Basis Keeper Alert",
          text: fullMessage,
        }),
      });
      if (!res.ok) {
        logger.warn("Resend email delivery failed", { status: res.status });
      }
    } catch (err) {
      logger.warn("Failed to send Resend email alert", {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  // Webhook (Slack/Discord)
  const webhookUrl = process.env["SLACK_WEBHOOK_URL"] ?? process.env["DISCORD_WEBHOOK_URL"];
  if (!webhookUrl) return;

  try {
    const body = isDiscordWebhook(webhookUrl)
      ? JSON.stringify({
          content: `🚨 **Basis Keeper Alert**\n${message}${errorStr ? `\n\`\`\`${errorStr}\`\`\`` : ""}`,
        })
      : JSON.stringify({
          text: `🚨 *Basis Keeper Alert*\n${message}${errorStr ? `\n\`\`\`${errorStr}\`\`\`` : ""}`,
        });

    const res = await fetch(webhookUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
    });

    if (!res.ok) {
      logger.warn("Webhook delivery failed", { status: res.status });
    }
  } catch (err) {
    logger.warn("Failed to send webhook alert", {
      error: err instanceof Error ? err.message : String(err),
    });
  }
}

function isDiscordWebhook(url: string): boolean {
  return url.includes("discord.com");
}

export async function checkStaleness(
  oracle: { isStale: (token: string, maxAge: bigint) => Promise<boolean> },
  tokens: string[],
  maxAge: number
): Promise<void> {
  for (const token of tokens) {
    const stale = await oracle.isStale(token, BigInt(maxAge));
    if (stale) {
      await sendAlert(`STALE SCORE: ${token} not updated in ${maxAge}s`);
    }
  }
}
