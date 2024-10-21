/**
 * Holds the information related to an event parsing error.
 * This class attempts to extract the event replay ID from the event that caused the error.
 * @alias EventParseError
 * @global
 */
export default class EventParseError extends Error {
  /**
   * The cause of the error.
   * @type {Error}
   * @public
   */
  public cause: Error

  /**
   * The replay ID of the event at the origin of the error.
   * Could be undefined if we're not able to extract it from the event data.
   * @type {number | undefined}
   * @public
   */
  public replayId?: number

  /**
   * The un-parsed event data at the origin of the error.
   * @type {unknown}
   * @public
   */
  public event?: unknown

  /**
   * The latest replay ID that was received before the error.
   * There could be more than one event between the replay ID and the event causing the error if the events were processed in batch.
   * @type {number | undefined}
   * @public
   */
  public latestReplayId?: number

  /**
   * Builds a new ParseError error.
   * @param {string} message The error message.
   * @param {Error} cause The cause of the error.
   * @param {number} replayId The replay ID of the event at the origin of the error.
   * Could be undefined if we're not able to extract it from the event data.
   * @param {unknown} event The un-parsed event data at the origin of the error.
   * @param {number} latestReplayId The latest replay ID that was received before the error.
   * @protected
   */
  constructor(
    message: string,
    cause: Error,
    replayId?: number,
    event?: unknown,
    latestReplayId?: number
  ) {
    super(message)
    this.cause = cause
    this.replayId = replayId
    this.event = event
    this.latestReplayId = latestReplayId
  }
}
