-- Detection regression gate: CI fails if any rule drops below the agreed precision/recall.
-- Change a threshold in transforms.py, and this tells you whether you made things worse.
select rule, precision, recall
from {{ ref('mart_detection_quality') }}
where attacks_total > 0
  and (coalesce(precision, 0) < {{ var('min_precision') }}
       or coalesce(recall, 0) < {{ var('min_recall') }})
