<?php

/**
 * Gearman task pool
 *
 * Runs multiple tasks in parallel
 *
 * @copyright 2013, Mikhail Yurasov <me@yurasov.me>
 */

namespace mym\GearmanTools;

class GearmanTaskPool
{
  private $servers = '127.0.0.1:4730';
  private $maxTasks = 10;
  private $functionName = '';
  private $workloadCallback;
  private $taskCallback;

  //

  private $tasks = [];

  /**
   * @var \GearmanClient
   */
  private $gc;

  private function init()
  {
    if (!$this->gc) {
      $this->gc = new \GearmanClient();
      $this->gc->addServer();
      $this->gc->setCompleteCallback([$this, 'onTaskComplete']);
    }
  }

  public function run()
  {
    $this->init();
    $this->addTasks();
    $this->gc->runTasks();
  }

  public function onTaskComplete(\GearmanTask $task)
  {
    if (is_callable($this->taskCallback)) {
      call_user_func($this->taskCallback, $task);
    }

    // remove task from the pool
    $unique = (int) $task->unique();
    unset($this->tasks[$unique]);

    // add more tasks
    $this->addTasks();
  }

  private function addTasks()
  {
    static $unique = 0;

    $tasksToAdd = $this->maxTasks - count($this->tasks);

    for ($i = 0; $i < $tasksToAdd; $i++) {

      $workload = call_user_func($this->workloadCallback);

      if ($workload === false) {
        return false;
      }

      $task = $this->gc->addTask(
        $this->functionName,
        (string) $workload,
        null,
        $unique
      );

      $this->tasks[$unique] = $task;
      $unique++;
    }

    return $tasksToAdd;
  }

  // <editor-fold defaultstate="collapsed" desc="accessors">

  public function getMaxTasks()
  {
    return $this->maxTasks;
  }

  public function setMaxTasks($maxTasks)
  {
    $this->maxTasks = $maxTasks;
  }

  public function getFunctionName()
  {
    return $this->functionName;
  }

  public function setFunctionName($functionName)
  {
    $this->functionName = $functionName;
  }

  public function getWorkloadCallback()
  {
    return $this->workloadCallback;
  }

  /**
   * Set callback for workload
   *
   * @param callable $workloadCallback function() { return (string) $workload | false (stop); }
   */
  public function setWorkloadCallback($workloadCallback)
  {
    $this->workloadCallback = $workloadCallback;
  }

  public function getTaskCallback()
  {
    return $this->taskCallback;
  }

  /**
   * Set callback for task completion
   *
   * @param callable $taskCallback function(\GearmanTask $task) ...
   */
  public function setTaskCallback($taskCallback)
  {
    $this->taskCallback = $taskCallback;
  }

  public function getServers()
  {
    return $this->servers;
  }

  public function setServers($servers)
  {
    $this->servers = $servers;
  }

  // </editor-fold>
}