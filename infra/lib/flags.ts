/**
 * Switches for features that ship off and are turned on by changing one line
 * here and deploying.
 */

/**
 * The Monday weekly summary email. While false, the schedule is disabled AND
 * the worker refuses to send it (WEEKLY_SUMMARY_ENABLED=false), so nobody gets
 * one. Test sends (`weekly_summary_test` on the ticket queue) work either way.
 */
export const WEEKLY_SUMMARY_ENABLED = true;

/**
 * While true, the Monday summary goes only to teams that have a rollout list
 * (settings item SETTINGS#WEEKLY_SUMMARY_ROLLOUT, attribute only_emails), and
 * only to the people on it. Teams without a list get nothing. While false,
 * every active team's members get it, lists are ignored, and each person can
 * still turn it off in their notification settings or with the email's
 * unsubscribe link.
 */
export const WEEKLY_SUMMARY_LISTED_TEAMS_ONLY = false;
