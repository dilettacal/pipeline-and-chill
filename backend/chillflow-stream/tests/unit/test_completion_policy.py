"""
Unit tests for CompletionPolicy.

Tests the business logic for determining trip completion without external dependencies.
"""

from stream.events import EventType


class TestCompletionPolicy:
    """Unit tests for CompletionPolicy."""

    def test_required_events(self):
        """Test CompletionPolicy defines required events correctly."""
        from stream.completion_policy import CompletionPolicy

        # Should define the required events
        required = CompletionPolicy.REQUIRED
        assert EventType.TRIP_STARTED in required
        assert EventType.TRIP_ENDED in required
        assert EventType.PAYMENT_PROCESSED in required

        # Should be able to check completion
        present_events = {EventType.TRIP_STARTED, EventType.TRIP_ENDED, EventType.PAYMENT_PROCESSED}

        is_complete = CompletionPolicy.is_complete_set(present_events)
        assert is_complete is True

        # Should detect incomplete trips
        incomplete_events = {
            EventType.TRIP_STARTED,
            EventType.TRIP_ENDED,
            # Missing PAYMENT_PROCESSED
        }

        is_complete = CompletionPolicy.is_complete_set(incomplete_events)
        assert is_complete is False

    def test_bitmask_operations(self):
        """Test CompletionPolicy works with bitmasks."""
        from stream.completion_policy import CompletionPolicy

        # Test bitmask operations
        mask = 0
        mask |= 1 << EventType.TRIP_STARTED.mask_bit()
        mask |= 1 << EventType.TRIP_ENDED.mask_bit()
        mask |= 1 << EventType.PAYMENT_PROCESSED.mask_bit()

        is_complete = CompletionPolicy.is_complete_mask(mask)
        assert is_complete is True

        # Test incomplete mask
        incomplete_mask = 0
        incomplete_mask |= 1 << EventType.TRIP_STARTED.mask_bit()
        incomplete_mask |= 1 << EventType.TRIP_ENDED.mask_bit()
        # Missing PAYMENT_PROCESSED

        is_complete = CompletionPolicy.is_complete_mask(incomplete_mask)
        assert is_complete is False

    def test_missing_events(self):
        """Test CompletionPolicy identifies missing events."""
        from stream.completion_policy import CompletionPolicy

        # Test with some events missing
        present_events = {EventType.TRIP_STARTED, EventType.TRIP_ENDED}
        missing = CompletionPolicy.missing_from(present_events)

        assert EventType.PAYMENT_PROCESSED in missing
        assert len(missing) == 1

        # Test with all events present
        all_events = {EventType.TRIP_STARTED, EventType.TRIP_ENDED, EventType.PAYMENT_PROCESSED}
        missing = CompletionPolicy.missing_from(all_events)

        assert len(missing) == 0
