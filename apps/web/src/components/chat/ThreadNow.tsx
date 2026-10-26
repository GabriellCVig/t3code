import {
  CheckIcon,
  ChevronDownIcon,
  CircleDotIcon,
  CircleIcon,
  ListTodoIcon,
  RefreshCwIcon,
} from "lucide-react";
import { useId, useState, type ReactNode } from "react";

import { formatDuration } from "../../session-logic";
import { threadSyncLabel, type ThreadSyncPhase } from "../../threadSync";
import { cn } from "~/lib/utils";
import { NoticeText } from "./ComposerBannerStack";

/** A line of thread state for the end of the timeline. */
export interface ThreadNowItem {
  readonly id: string;
  readonly icon: ReactNode;
  readonly title: ReactNode;
  readonly description?: ReactNode;
  readonly actions?: ReactNode;
}

export interface ThreadTasksProgress {
  readonly step: string;
  readonly completedSteps: number;
  readonly totalSteps: number;
  readonly live: boolean;
}

export interface ThreadTaskStep {
  readonly durationMs?: number;
  readonly step: string;
  readonly status: "pending" | "inProgress" | "completed";
}

/** Matches ThreadStatusLine, so every line at the end of the timeline reads as one family. */
const lineClassName =
  "mx-auto flex w-full min-w-0 max-w-(--chat-content-max-width) items-center gap-1.5 px-1 pb-2 text-xs text-muted-foreground [&_svg]:size-3.5 [&_svg]:shrink-0";

/** One line of thread state at the end of the timeline: icon, title, detail, then actions. */
function ThreadNowLine({ item }: { readonly item: ThreadNowItem }) {
  return (
    <div className={lineClassName} data-thread-now-line="true">
      <span aria-hidden className="flex size-3.5 shrink-0 items-center justify-center">
        {item.icon}
      </span>
      <span className="shrink-0 text-foreground/80">{item.title}</span>
      {item.description ? (
        <NoticeText
          detailsLabel={
            typeof item.title === "string" ? `Show details: ${item.title}` : "Show details"
          }
          className="min-w-0 truncate"
        >
          {item.description}
        </NoticeText>
      ) : null}
      {item.actions ? (
        <>
          <span aria-hidden>·</span>
          <span className="flex shrink-0 items-center gap-1.5">{item.actions}</span>
        </>
      ) : null}
    </div>
  );
}

function ThreadSyncLine({ phase }: { readonly phase: ThreadSyncPhase }) {
  return (
    <div className={lineClassName} role="status" data-composer-sync-status={phase}>
      <RefreshCwIcon aria-hidden />
      <span className="truncate">{threadSyncLabel(phase)}</span>
    </div>
  );
}

const MAX_TASK_SEGMENTS = 10;

const taskStatusLabels = {
  pending: "Pending",
  inProgress: "Running",
  completed: "Completed",
} satisfies Record<ThreadTaskStep["status"], string>;

function keyedTaskSteps(steps: readonly ThreadTaskStep[]) {
  const occurrences = new Map<string, number>();
  return steps.map((step) => {
    const occurrence = occurrences.get(step.step) ?? 0;
    occurrences.set(step.step, occurrence + 1);
    return { key: `${step.step}:${occurrence}`, step };
  });
}

function TaskSegments({ steps }: { readonly steps: readonly ThreadTaskStep[] }) {
  if (steps.length <= 1 || steps.length > MAX_TASK_SEGMENTS) return null;
  return (
    <span aria-hidden className="flex w-12 shrink-0 items-center gap-0.5">
      {keyedTaskSteps(steps).map(({ key, step }) => (
        <span
          key={key}
          className={cn(
            "h-[3px] min-w-0 flex-1 rounded-full",
            step.status === "completed"
              ? "bg-success"
              : step.status === "inProgress"
                ? "bg-primary"
                : "bg-muted-foreground/25",
          )}
        />
      ))}
    </span>
  );
}

/** The agent's todo list: progress and the current step, opening to every step in place. */
function ThreadTasksLine({
  progress,
  steps,
}: {
  readonly progress: ThreadTasksProgress;
  readonly steps: readonly ThreadTaskStep[];
}) {
  const [expanded, setExpanded] = useState(false);
  const listId = useId();
  const done = progress.completedSteps >= progress.totalSteps;
  return (
    <div className="mx-auto w-full max-w-(--chat-content-max-width) pb-2">
      <button
        type="button"
        aria-expanded={expanded}
        aria-controls={listId}
        aria-label={`Tasks: ${progress.completedSteps} of ${progress.totalSteps} complete. Current task: ${progress.step}`}
        data-composer-tasks-badge="true"
        onClick={() => setExpanded((value) => !value)}
        className={cn(
          lineClassName,
          "cursor-pointer rounded-md pb-0 text-start hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring",
        )}
      >
        <ListTodoIcon aria-hidden />
        <span
          className={cn("shrink-0 tabular-nums", done ? "text-success" : "text-foreground/80")}
          data-composer-task-progress="true"
        >
          {progress.completedSteps}/{progress.totalSteps}
        </span>
        <TaskSegments steps={steps} />
        <span className="min-w-0 truncate" data-composer-task-current="true">
          {progress.step}
        </span>
        <ChevronDownIcon aria-hidden className={cn(expanded && "rotate-180")} />
      </button>
      {expanded ? (
        <ul
          id={listId}
          aria-label={`Task list. ${progress.completedSteps} of ${progress.totalSteps} complete.`}
          data-composer-tasks-list="true"
          className="grid gap-px px-1 pt-1.5"
        >
          {keyedTaskSteps(steps).map(({ key, step }) => (
            <li key={key} className="flex min-w-0 items-start gap-1.5 py-0.5 text-xs">
              <span
                aria-hidden
                className={cn(
                  "flex h-4 shrink-0 items-center [&>svg]:size-3.5",
                  step.status === "completed"
                    ? "text-success"
                    : step.status === "inProgress"
                      ? "text-primary"
                      : "text-muted-foreground/50",
                )}
              >
                {step.status === "completed" ? (
                  <CheckIcon />
                ) : step.status === "inProgress" ? (
                  <CircleDotIcon />
                ) : (
                  <CircleIcon />
                )}
              </span>
              <span
                className={cn(
                  "min-w-0 flex-1 wrap-anywhere",
                  step.status === "inProgress" ? "text-foreground" : "text-muted-foreground",
                )}
              >
                <span className="sr-only">{taskStatusLabels[step.status]}: </span>
                {step.step}
              </span>
              <span
                className="shrink-0 text-muted-foreground tabular-nums"
                data-composer-task-duration="true"
              >
                {step.durationMs !== undefined
                  ? formatDuration(step.durationMs)
                  : step.status === "inProgress" && progress.live
                    ? "now"
                    : null}
              </span>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}

/**
 * What the thread is doing now, at the end of the timeline: what needs you, background work,
 * tasks (or the sync status while messages load), then the goal.
 */
export function ThreadNow({
  attention,
  work,
  tasks,
  syncPhase,
  goal,
}: {
  /** Something that needs you elsewhere, such as a subagent's question. */
  readonly attention: ThreadNowItem | null;
  readonly work: ThreadNowItem | null;
  readonly tasks: { progress: ThreadTasksProgress; steps: readonly ThreadTaskStep[] } | null;
  readonly syncPhase: ThreadSyncPhase | null;
  readonly goal: ThreadNowItem | null;
}): ReactNode {
  return (
    <>
      {attention ? <ThreadNowLine item={attention} /> : null}
      {work ? <ThreadNowLine item={work} /> : null}
      {syncPhase ? (
        <ThreadSyncLine phase={syncPhase} />
      ) : tasks ? (
        <ThreadTasksLine progress={tasks.progress} steps={tasks.steps} />
      ) : null}
      {goal ? <ThreadNowLine item={goal} /> : null}
    </>
  );
}
