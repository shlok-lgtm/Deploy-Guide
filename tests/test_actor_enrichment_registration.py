"""
Test: actor_classification is registered in the enrichment pipeline.
"""
import unittest
from unittest.mock import patch, MagicMock


class TestActorClassificationRegistered(unittest.TestCase):

    @patch("app.enrichment_worker.make_db_gate", return_value=lambda: True)
    def test_actor_classification_in_pipeline(self, mock_gate):
        from app.enrichment_worker import run_enrichment_pipeline, EnrichmentPipeline

        # Build the pipeline by inspecting what run_enrichment_pipeline registers.
        # The pipeline is built inside run_enrichment_pipeline, so we need to
        # intercept the EnrichmentPipeline.add calls.
        added_tasks = []
        original_add = EnrichmentPipeline.add

        def capture_add(self, task):
            added_tasks.append(task)
            return original_add(self, task)

        with patch.object(EnrichmentPipeline, "add", capture_add):
            with patch.object(EnrichmentPipeline, "run", return_value=[]):
                import asyncio
                try:
                    asyncio.run(run_enrichment_pipeline())
                except Exception:
                    pass

        task_names = [t.name for t in added_tasks]
        assert "actor_classification" in task_names, \
            f"actor_classification not found in pipeline tasks: {task_names}"

        actor_task = next(t for t in added_tasks if t.name == "actor_classification")
        assert actor_task.group == "wallet"
        assert actor_task.timeout_seconds == 600
        assert actor_task.priority == 2


if __name__ == "__main__":
    unittest.main()
