/**
 * The scheduled watch on the corpus revision.
 *
 * <p>{@link dev.sbs.data.poller.CorpusPoller} asks the corpus whether it moved and records which
 * documents' fingerprints changed. It holds that state in memory and rebuilds nothing: an external
 * change signal carries a fingerprint, never an instruction, so noticing a change and acting on one
 * stay separate.
 */
package dev.sbs.data.poller;
