/**
 * Switches for features that ship off and are turned on by changing one line
 * here and deploying.
 */

/**
 * The Monday weekly summary email. While false, the schedule is disabled AND
 * the worker refuses to send it (WEEKLY_SUMMARY_ENABLED=false), so nobody gets
 * one. Test sends (`weekly_summary_test` on the ticket queue) work either way.
 */
export const WEEKLY_SUMMARY_ENABLED = false;
