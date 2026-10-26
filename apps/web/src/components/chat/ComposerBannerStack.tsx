import { InfoIcon } from "lucide-react";
import { useLayoutEffect, useRef, useState, type ReactNode } from "react";

import { cn } from "~/lib/utils";
import { Button } from "../ui/button";
import { Popover, PopoverPopup, PopoverTrigger } from "../ui/popover";
import { ComposerBanner, type ComposerBannerVariant } from "./ComposerBanner";
import { observeResize } from "~/lib/observeResize";

/**
 * Where a notice sits above the composer. The machine comes first, then what needs you; the
 * order is fixed so rows never trade places as notices come and go.
 */
export type ComposerBannerGroup = "environment" | "attention";

export interface ComposerBannerStackItem {
  readonly id: string;
  readonly variant: ComposerBannerVariant;
  readonly group?: ComposerBannerGroup;
  readonly icon: ReactNode;
  readonly title: ReactNode;
  readonly description?: ReactNode;
  readonly children?: ReactNode;
  readonly actions?: ReactNode;
  readonly dismissLabel?: string;
  readonly onDismiss?: () => void;
}

const GROUPS = ["environment", "attention"] as const;

/** Notices about sending from this composer, on one attached surface. */
export function ComposerBannerStack({
  items,
}: {
  readonly items: ReadonlyArray<ComposerBannerStackItem>;
}) {
  const bands = GROUPS.map((group) =>
    items.filter((item) => (item.group ?? "attention") === group),
  ).filter((band) => band.length > 0);
  if (bands.length === 0) return null;
  return (
    <ComposerBanner.Attachment data-composer-banner-stack="true">
      <ComposerBanner.Root
        role="region"
        aria-label="Notices"
        density="comfortable"
        data-chat-composer-collapsed-controls="true"
        className={cn(
          "[&>[data-composer-shelf-band]+[data-composer-shelf-band]]:border-t",
          // The first row's corner button nests inside the shelf's top-right radius.
          "[&>[data-composer-shelf-band]:first-child>[data-composer-shelf-row]:first-child>[data-composer-banner-row]>[data-slot=composer-banner-actions]>[data-slot=button]:last-child]:rounded-se-xl",
        )}
      >
        {bands.map((band) => (
          <div
            key={band[0]!.group ?? "attention"}
            data-composer-shelf-band="true"
            className="-mx-1 border-(--chat-composer-attached-outline) px-1 py-0.5 *:transition-opacity *:duration-150 *:ease-out *:starting:opacity-0 motion-reduce:*:transition-none"
          >
            {band.map((item) => (
              <ShelfRow key={item.id} item={item} />
            ))}
          </div>
        ))}
      </ComposerBanner.Root>
    </ComposerBanner.Attachment>
  );
}

export function NoticeText({
  children,
  className,
  detailsLabel,
}: {
  readonly children: ReactNode;
  readonly className: string;
  readonly detailsLabel: string;
}) {
  const textRef = useRef<HTMLSpanElement>(null);
  const detailsRef = useRef<HTMLButtonElement>(null);
  const [showDetails, setShowDetails] = useState(false);

  useLayoutEffect(() => {
    const text = textRef.current;
    if (!text) return;
    const measure = () => {
      // Ignore the space taken by the details button itself so it cannot
      // sustain its own overflow after the text would otherwise fit.
      const recoveredWidth = detailsRef.current ? detailsRef.current.offsetWidth + 4 : 0;
      const hidden = getComputedStyle(text).position === "absolute";
      setShowDetails(
        hidden ||
          [text, ...text.querySelectorAll("*")].some(
            (element) => element.scrollWidth > element.clientWidth + recoveredWidth,
          ),
      );
    };
    measure();
    const stopObserving = observeResize(text, measure);
    const mutations = new MutationObserver(measure);
    mutations.observe(text, { childList: true, subtree: true, characterData: true });
    return () => {
      stopObserving();
      mutations.disconnect();
    };
  }, []);

  return (
    <span className="contents">
      <span ref={textRef} className={className}>
        {children}
      </span>
      {showDetails ? (
        <Popover>
          <PopoverTrigger
            openOnHover
            render={
              <Button
                ref={detailsRef}
                size="icon-xs"
                variant="ghost-muted"
                aria-label={detailsLabel}
                className="flex-none"
              />
            }
          >
            <InfoIcon />
          </PopoverTrigger>
          <PopoverPopup
            aria-label="Notice details"
            tooltipStyle
            side="top"
            className="max-w-[min(30rem,calc(100vw-2rem))] whitespace-normal wrap-anywhere [--inline-button-text-align:start] [--inline-button-white-space:normal] [&_[data-slot=inline-button]]:max-w-full"
          >
            <ComposerBanner.Scroll className="max-h-[min(var(--available-height),24rem,40dvh)]">
              {children}
            </ComposerBanner.Scroll>
          </PopoverPopup>
        </Popover>
      ) : null}
    </span>
  );
}

function ShelfRow({ item }: { readonly item: ComposerBannerStackItem }) {
  const problem = item.variant === "error" || item.variant === "warning";
  return (
    <div
      role={problem ? "alert" : undefined}
      data-composer-shelf-row={item.variant}
      className={cn(
        problem && "rounded-xl",
        item.variant === "error" &&
          "bg-destructive/8 [&>[data-composer-banner-row]>[data-slot=composer-banner-icon]]:text-destructive-foreground",
        item.variant === "warning" &&
          "bg-warning/8 [&>[data-composer-banner-row]>[data-slot=composer-banner-icon]]:text-warning-foreground",
      )}
    >
      <ComposerBanner.Row layout="wrap-actions">
        <ComposerBanner.Icon className="h-(--composer-banner-icon-column) self-start">
          {item.icon}
        </ComposerBanner.Icon>
        <ComposerBanner.Content className="whitespace-nowrap">
          <NoticeText
            detailsLabel={
              typeof item.title === "string"
                ? `Show full title: ${item.title}`
                : "Show title details"
            }
            className="min-w-0 truncate font-medium leading-7 sm:leading-6"
          >
            {item.title}
          </NoticeText>
          {item.description ? (
            <NoticeText
              detailsLabel={
                typeof item.title === "string"
                  ? `Show details: ${item.title}`
                  : "Show notice details"
              }
              className={cn(
                "w-0 min-w-0 grow truncate @max-[400px]:sr-only",
                // Muted text falls under 4.5:1 on the light problem tints.
                problem ? "text-foreground/65" : "text-muted-foreground",
              )}
            >
              {item.description}
            </NoticeText>
          ) : null}
        </ComposerBanner.Content>
        {item.actions || item.onDismiss ? (
          <ComposerBanner.Actions>
            {item.actions}
            {item.onDismiss ? (
              <ComposerBanner.Dismiss
                aria-label={item.dismissLabel ?? "Dismiss warning"}
                onClick={item.onDismiss}
              />
            ) : null}
          </ComposerBanner.Actions>
        ) : null}
      </ComposerBanner.Row>
      {item.children ? <ComposerBanner.Children>{item.children}</ComposerBanner.Children> : null}
    </div>
  );
}
