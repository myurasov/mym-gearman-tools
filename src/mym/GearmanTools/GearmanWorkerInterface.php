<?php

namespace mym\GearmanTools;

use Psr\Log\LoggerAwareInterface;

interface GearmanWorkerInterface extends LoggerAwareInterface
{
  public function run(\GearmanJob $job);
}
