package com.github.highcumontoa.servicediscoverygatewayjava.downstream;

import java.util.List;
import java.util.Objects;

/**
 * 按调用次数依次返回预设行为，最后一个行为持续生效。
 * 用于可复现地验证“连续失败 N 次后熔断/摘除、第 N+1 次恢复”等链路。
 */
public class ScriptedBehavior {

    private final Behavior[] behaviors;
    private int cursor;

    private ScriptedBehavior(Behavior[] behaviors) {
        this.behaviors = behaviors;
    }

    public static ScriptedBehavior always(Behavior behavior) {
        return new ScriptedBehavior(new Behavior[]{Objects.requireNonNull(behavior)});
    }

    public static ScriptedBehavior of(List<Behavior> behaviors) {
        if (behaviors == null || behaviors.isEmpty()) {
            throw new IllegalArgumentException("behaviors must not be empty");
        }
        return new ScriptedBehavior(behaviors.toArray(new Behavior[0]));
    }

    /** 返回下一个行为；脚本耗尽后固定返回最后一个。 */
    public synchronized Behavior next() {
        Behavior behavior = behaviors[Math.min(cursor, behaviors.length - 1)];
        if (cursor < behaviors.length - 1) {
            cursor++;
        }
        return behavior;
    }

    public synchronized void reset() {
        cursor = 0;
    }
}
