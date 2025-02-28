//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!tmp_result_.empty()) {
    *tuple = tmp_result_.front();
    tmp_result_.pop();
    return true;
  }

  Tuple left_tuple;
  RID left_rid;

  Tuple right_tuple;
  RID right_rid;

  if (!left_executor_->Next(&left_tuple,&left_rid)) {
    return false;
  }

  right_executor_->Init();
  while(right_executor_->Next(&right_tuple,&right_rid)) {
    if (plan_->Predicate() == nullptr || plan_->Predicate()->
        EvaluateJoin(&left_tuple,left_executor_->GetOutputSchema(),
                                                      &right_tuple, right_executor_->GetOutputSchema())
        .GetAs<bool>()) {
      std::vector<Value> output;
      for (const auto &col : GetOutputSchema()->GetColumns()) {
        output.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                     right_executor_->GetOutputSchema()));
      }
      tmp_result_.push(Tuple(output, GetOutputSchema()));
    }
  }
  return Next(tuple,rid);
}

}  // namespace bustub
