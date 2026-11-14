import { MongoServerError, ObjectId } from "mongodb";
import { collections, type Assignment } from "./db.js";

export type AssignResult =
  | { kind: "assigned"; assignment: Assignment }
  | { kind: "judge-busy" } // the judge already holds a QUEUED assignment
  | { kind: "none-available" }; // no project is eligible for this judge right now

/**
 * TODO: implement. See the README for the rules.
 *
 * `judgeId` is already validated to exist by the route. Use `collections()` from ./db.js.
 * Add any helpers you need.
 */
export async function assignNext(judgeId: string): Promise<AssignResult> {
    const { projects, assignments } = collections();

  await Promise.all([
    assignments.createIndex({ judgeId: 1 }, { unique: true, partialFilterExpression: { status: "QUEUED" } }), 
    assignments.createIndex({ projectId: 1 }, { unique: true, partialFilterExpression: { status: "QUEUED" } }), 
    assignments.createIndex({ judgeId: 1, projectId: 1 }, { unique: true }), 
  ]);

  const mine = await assignments.find({ judgeId }).toArray();
  if (mine.some((a) => a.status === "QUEUED")) return { kind: "judge-busy" };
  const seen = new Set(mine.map((a) => a.projectId));

  const stats = new Map(
    (
      await assignments
        .aggregate<{ _id: string; completed: number; queued: number }>([
          {
            $group: {
              _id: "$projectId",
              completed: { $sum: { $cond: [{ $eq: ["$status", "COMPLETED"] }, 1, 0] } },
              queued: { $sum: { $cond: [{ $eq: ["$status", "QUEUED"] }, 1, 0] } },
            },
          },
        ])
        .toArray()
    ).map((r) => [r._id, r])
  );

  const completedOf = (id: string) => stats.get(id)?.completed ?? 0;
  const candidates = (await projects.find().toArray())
    .filter((p) => !seen.has(p._id) && !stats.get(p._id)?.queued)
    .map((p) => ({ p, r: Math.random() }))
    .sort((a, b) => a.r - b.r)
    .map(({ p }) => p)
    .sort((a, b) => completedOf(a._id) - completedOf(b._id));

  for (const project of candidates) {
    const doc: Assignment = {
      _id: new ObjectId(),
      judgeId,
      projectId: project._id,
      status: "QUEUED",
      createdAt: new Date(),
    };
    try {
      await assignments.insertOne(doc);
      return { kind: "assigned", assignment: doc };
    } catch (err) {
      if (!(err instanceof MongoServerError) || err.code !== 11000) throw err;
      const keys = Object.keys(err.keyPattern ?? {});
      if (keys.length === 1 && keys[0] === "judgeId") return { kind: "judge-busy" };
    }
  }
  return { kind: "none-available" };
}
