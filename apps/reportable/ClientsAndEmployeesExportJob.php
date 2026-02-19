<?php

namespace common\jobs;

use common\models\Msp;
use common\components\payroll\BaseTimesheetAnalyzer;
use common\models\Employee;
use common\components\Utils;
use common\models\Client;
use common\models\Event;
use common\models\TravelRate;
use common\models\HistoryEntry;
use common\models\Timesheet;
use yii\base\BaseObject;
use yii\helpers\Inflector;
use yii\queue\JobInterface;
use yii\helpers\ArrayHelper;
use common\models\Certification;
use common\dictionaries\USStates;
use common\models\EmployeeOtherWork;
use common\models\ClientPurchaseOrder;
use common\models\Shift;
use common\models\ShiftEmployee;
use common\models\ShiftPosition;
use api\models\form\PayrollExportForm;
use PhpOffice\PhpSpreadsheet\Cell\Coordinate;
use PhpOffice\PhpSpreadsheet\Spreadsheet;
use PhpOffice\PhpSpreadsheet\Style\Alignment;
use PhpOffice\PhpSpreadsheet\Style\NumberFormat;
use PhpOffice\PhpSpreadsheet\Writer\Xlsx;
use common\components\payroll\TimesheetException;
use common\components\payroll\BaseMealBreakPolicy;

class ClientsAndEmployeesExportJob extends BaseObject implements JobInterface
{
    public $jobId;
    public $type;
    public $startDate;
    public $endDate;
    public $clientId;
    public $venueId;
    public $employeeId;
    public $tipsMultiplier;
    public $mspId;

    public function isLong()
    {
        return true;
    }

    public function execute($queue)
    {
        $type = $this->type;
        $start_date = $this->startDate;
        $end_date = $this->endDate;
        $export_client_id = $this->clientId;
        $export_venue_id = $this->venueId;
        $employee_id = $this->employeeId;

        $createFile
            = function ($export_client_id, $export_venue_id = null, $employee_id = null, $oneVenue = true)
                use (&$type, &$start_date, &$end_date) {
                    $where = [];

                    if (PayrollExportForm::TYPE_CLIENTS == $type) {
                        if ($export_client_id) {
                            $where['client.client_id'] = $export_client_id;
                        }

                        if ($export_venue_id) {
                            $where['venue.venue_id'] = $export_venue_id;
                        }

                        if ($this->mspId) {
                            $where['client.msp_id'] = $this->mspId;
                        }

                        $order = [
                            'venue.name' => SORT_ASC,
                            "FIELD(day, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')" => SORT_ASC,
                            'event.title' => SORT_ASC
                        ];
                    } elseif (2 == $type) {
                        // for all client

                        $order = ['client.name' => SORT_ASC, 'venue.name' => SORT_ASC, "FIELD(day, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')" => SORT_ASC, 'event.title' => SORT_ASC];
                    } elseif (PayrollExportForm::TYPE_EMPLOYEE == $type) {
                        $where = [
                            'shift_employee.employee_id' => $employee_id
                        ];

                        $order = ['client.name' => SORT_ASC, 'venue.name' => SORT_ASC, "FIELD(day, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')" => SORT_ASC, 'event.title' => SORT_ASC];
                    } elseif (
                        PayrollExportForm::TYPE_TIMESHEET_VERIFICATION == $type
                        || PayrollExportForm::TYPE_TIMESHEET_VERIFICATION_CLIENTS == $type
                    ) {
                        if ($export_client_id) {
                            $where['client.client_id'] = $export_client_id;
                        }

                        if ($export_venue_id) {
                            $where['venue.venue_id'] = $export_venue_id;
                        }

                        $order = ['client.name' => SORT_ASC, 'venue.name' => SORT_ASC, "FIELD(day, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')" => SORT_ASC, 'event.title' => SORT_ASC];
                    } else {
                        $order = ['client.name' => SORT_ASC, 'venue.name' => SORT_ASC, "FIELD(day, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')" => SORT_ASC, 'event.title' => SORT_ASC];
                    }

                    $shiftEmployeeQuery
                        = ShiftEmployee::find()
                            ->select([
                                'DAYNAME(event.date) as day',
                                'shift_employee.*',
                                'event.event_id',
                                'event.title',
                                'venue.name',
                                'venue.service_charge',
                                'client.client_id',
                                'client.name',
                                'client.wc_id',
                            ])
                            ->joinWith([
                                'event',
                                'event.client',
                                'event.venue',
                                'timesheet',
                            ])
                            ->with(
                                'payrollNotes',
                                'event.venue.positions',
                                'event.venue.positions.position',
                                'shiftPosition',
                                'shiftPosition.shift',
                                'shiftPosition.position',
                                'shiftPosition.position.venuePosition',
                                'event.venue.staffingmanager'
                            )
                            ->andWhere(['between', 'date', $start_date, $end_date])
                            ->andWhere($where)
                            ->orderBy($order);

                    if (
                            PayrollExportForm::TYPE_TIMESHEET_VERIFICATION == $type
                            || PayrollExportForm::TYPE_TIMESHEET_VERIFICATION_CLIENTS == $type
                            ) {
                        $shiftEmployeeQuery->andWhere(
                            "(shift_employee.confirmed AND NOT shift_employee.cancel_reason) 
                            OR timesheet.client_min_bill 
                            OR timesheet.employee_min_pay"
                        );
                    } else {
                        $shiftEmployeeQuery->andValidForPayroll();
                    }

                    $shift_emp = $shiftEmployeeQuery->all();

                    if (empty($shift_emp)) {
                        return null;
                    }

                    foreach ($shift_emp as $value) {
                        $client = $value->event->client;
                        $msp = $client->msp;

                        if ($msp) {
                            $client_name = "{$msp->name} {$client->name}";
                        } else {
                            $client_name = $client->name;
                        }

                        $venue_name = $value->event->venue->name;
                        $payment_type = $client->getPaymentType();
                        $payment_Note = $client->pay_notes;
                    }

                    $isAllEmployeesReport = in_array($type, [
                        PayrollExportForm::TYPE_ALL_CLIENTS_EMPLOYEES,
                    ]);

                    $venueColumn = $isAllEmployeesReport ? 'Venue Code' : 'Venue';

                    $headers = [
                        'Day',
                        'Date',
                        'WC',
                        'Client',
                        'Markup',
                        'Event State',
                        $venueColumn,
                        'Event',
                        'Position',
                        'Code',
                        'AM/PM',
                        '#Emp',
                        'First Name',
                        'Last Name',
                        'Work State',
                        'Meal H (e)',
                        'Reg H (e)',
                        'OT H (e)',
                        'DT H (e)',
                        'Reg Rate (e)',
                        'Non-Worked Hours (e)',
                        'Cert Cost (e)',
                        'Pay Rate',
                        'OT Rate',
                        'DT Rate',
                        'Std Pay',
                        'OT P',
                        'DT P',
                        'Bonus',
                        'Additional Shift Pay',
                        'Tip (e)',
                        'Park (e)',
                        'Travel (e)',
                        'Travel Mileage Pay',
                        'Service (e)',
                        'Meal 1 (e)',
                        'Meal 2 (e)',
                        'Non-Worked Pay (e)',
                        'Reimb Pay (e)',
                        'Meal Break Minutes (e)',
                        'Meal Break Reason (e)',
                        '2nd Meal Break Reason (e)',
                        'Gross Pay',
                        'P.A.W',
                        'Bill Rate',
                        'Reg H (c)',
                        'OT H (c)',
                        'DT H (c)',
                        'Reg Rate (c)',
                        'Non-Worked Hours (c)',
                        'Reg B',
                        'OT R',
                        'DT R',
                        'Tip (c)',
                        'Park (c)',
                        'Travel (c)',
                        'Travel Mileage Bill',
                        'Service (c)',
                        'Meal (c)',
                        'Non-Worked Bill (c)',
                        'Meal Break Minutes (c)',
                        'Meal Break Reason (c)',
                        '2nd Meal Break Reason (c)',
                        'Total Bill',
                        'Reconciled',
                    ];

                    if (
                        PayrollExportForm::TYPE_TIMESHEET_VERIFICATION == $type
                        || PayrollExportForm::TYPE_TIMESHEET_VERIFICATION_CLIENTS == $type
                    ) {
                        $headers = [
                            'Day',
                            'Date',
                            'WC',
                            'Client',
                            'Markup',
                            'Venue',
                            'Event',
                            'Position',
                            'Code',
                            '#Emp',
                            'First Name',
                            'Last Name',
                            'Work State',
                            'Reg H (c)',
                            'OT H (c)',
                            'DT H (c)',
                            'Reg Rate (c)',
                            'Non-Worked Hours (c)',
                            'Cert Cost (e)',
                            'OT R',
                            'DT R',
                            'Tip (c)',
                            'Park (c)',
                            'Travel (c)',
                            'Service (c)',
                            'Meal (c)',
                            'Non-Worked Bill (c)',
                            'Reimb Pay (e)',
                            'Bill Rate',
                            'Total Bill',
                            'Status',
                            'Cancellation Reason',
                            'Verification (c)',
                            'Verification (e)',
                        ];
                    } elseif ($isAllEmployeesReport) {
                        $headers[] = 'Position Level';
                        $headers[] = 'Start Date';
                        $headers[] = 'Employee Rating';
                        $headers[] = 'Client Rating';
                        $headers[] = 'WC rate';
                        $headers[] = 'MSP rate';
                        $headers[] = 'Staffing Manager';
                        $headers[] = 'Sales Executive';
                        $headers[] = 'Venue Name';
                        $headers[] = 'County of Venue';
                        $headers[] = '1st Event Date';
                        $headers[] = 'Client Won Date';
                        $headers[] = 'Prior Won Date';
                        $headers[] = 'Notes';
                        $headers[] = 'Status';
                        $headers[] = 'Industry';
                        $headers[] = 'Rehire Date';
                        $headers[] = 'Recruited By';
                        $headers[] = 'Cancellation Reason';

                        $headers[] = 'Surcharge Billed';
                        $headers[] = 'Minimum Billed';
                        $headers[] = 'Cancelled In Deadline';
                        $headers[] = 'Background';
                        $headers[] = 'Background Query';
                    } else {
                        $headers[] = 'Notes';
                        $headers[] = 'Status';
                    }

                    $headers[] = 'Employee Notes';
                    $headers[] = 'Client Notes';

                    $indicesByHeader = array_map(function ($arrayIndex) {
                        return $arrayIndex + 1;
                    }, array_flip($headers));

                    $columnNamesByHeader = [];
                    foreach ($headers as $header) {
                        $columnNamesByHeader[$header] = Coordinate::stringFromColumnIndex($indicesByHeader[$header]);
                    }

                    $lastColumnIndex = count($headers);
                    $lastColumnName = Coordinate::stringFromColumnIndex($lastColumnIndex);

                    $spreadsheet = new Spreadsheet();
                    $sheet = $spreadsheet->getActiveSheet();

                    $sheet->getPageMargins()->setTop(0);
                    $sheet->getPageMargins()->setRight(0);
                    $sheet->getPageMargins()->setLeft(0);
                    $sheet->getPageMargins()->setBottom(0);
                    $sheet->getPageMargins()->setHeader(0);
                    $sheet->getPageMargins()->setFooter(0);

                    $bold = [
                        'font' => [
                            'bold' => true,
                        ],
                    ];

                    foreach ($headers as $header) {
                        $columnName = $columnNamesByHeader[$header];
                        $sheet->setCellValue("{$columnName}1", $header);
                    }

                    $sheet->getRowDimension('1')->setRowHeight(40);

                    $sheet->getColumnDimension('A')->setWidth(12);

                    foreach (range(2, $lastColumnIndex) as $columnIndex) {
                        $columnID = \PhpOffice\PhpSpreadsheet\Cell\Coordinate::stringFromColumnIndex($columnIndex);

                        $sheet->getColumnDimension($columnID)->setAutoSize(true);
                    }

                    if (
                        PayrollExportForm::TYPE_TIMESHEET_VERIFICATION == $type
                        || PayrollExportForm::TYPE_TIMESHEET_VERIFICATION_CLIENTS == $type
                    ) {
                        foreach (range($indicesByHeader['Reg H (c)'], $indicesByHeader['Total Bill']) as $columnIndex) {
                            $columnID = \PhpOffice\PhpSpreadsheet\Cell\Coordinate::stringFromColumnIndex($columnIndex);

                            $sheet->getStyle($columnID)->getNumberFormat()->setFormatCode(NumberFormat::FORMAT_NUMBER_COMMA_SEPARATED1);
                        }
                    } else {
                        foreach (range($indicesByHeader['Reg H (e)'], $indicesByHeader['Total Bill']) as $columnIndex) {
                            $columnID = \PhpOffice\PhpSpreadsheet\Cell\Coordinate::stringFromColumnIndex($columnIndex);

                            $sheet->getStyle($columnID)->getNumberFormat()->setFormatCode(NumberFormat::FORMAT_NUMBER_COMMA_SEPARATED1);
                        }

                        $sheet->getStyle($columnNamesByHeader['Additional Shift Pay'])
                            ->getNumberFormat()
                            ->setFormatCode(NumberFormat::FORMAT_NUMBER_COMMA_SEPARATED1);
                    }

                    $sheet->getStyle("A1:{$lastColumnName}1")
                            ->getAlignment()
                            ->setHorizontal(Alignment::HORIZONTAL_CENTER);

                    $style = [
                        'borders' => [
                            'allBorders' => [
                                'borderStyle' => \PhpOffice\PhpSpreadsheet\Style\Border::BORDER_THIN,
                                'color' => ['argb' => '00000'],
                            ],
                        ],
                    ];

                    $sheet->getStyle("A1:{$lastColumnName}1")->applyFromArray($style);
                    $sheet->getStyle("A1:{$lastColumnName}1")->getFont()->setSize(12);

                    $total_reg_hours = 0;
                    $total_ot_hours = 0;
                    $total_dt_hours = 0;
                    $total_reg_salary = 0.00;
                    $total_ot_salary = 0.00;
                    $total_dt_salary = 0.00;
                    $totalNonWorkedSalary = 0.00;
                    $employee_meal_penalty_amount = 0;
                    $client_meal_penalty_amount = 0;
                    $total_tip = 0.00;
                    $grand_bill_amount = 0.00;
                    $total_paw = 0.00;
                    $gross_pay = 0.00;
                    $total_client_tip = 0.00;
                    $total_client_travel = 0.00;
                    $total_client_parking = 0.00;
                    $total_client_ot = 0.00;
                    $total_client_dt = 0.00;
                    $total_client_service = 0.00;
                    $total_client_oth = 0;
                    $total_client_dth = 0;
                    $total_emp_tip = 0.00;
                    $total_emp_parking = 0.00;
                    $total_emp_service = 0.00;
                    $total_emp_travel = 0.00;
                    $total_emp_ot = 0.00;
                    $total_emp_dt = 0.00;
                    $total_emp_meal_p = 0.00;
                    $total_client_meal_p = 0.00;
                    $total_emp_oth = 0;
                    $total_emp_dth = 0;
                    $event_name = '';
                    $total_gross_pay = 0.00;
                    $total_emp_meal = 0;
                    $total_client_meal = 0;

                    if (in_array($this->type, [PayrollExportForm::TYPE_CLIENTS, PayrollExportForm::TYPE_ALL_CLIENTS_EMPLOYEES])) {
                        $tipsMultiplier = $this->tipsMultiplier === null ? 1.1 : ((100 + $this->tipsMultiplier) / 100);
                    } else {
                        $tipsMultiplier = 1;
                    }

                    $usStateLabels = (new USStates())->getLabels();

                    // itrate the sheet rows
                    $index = 2;
                    $po = [];
                    foreach ($shift_emp as $e) {

                        /* @var $e ShiftEmployee */
                        $timesheet = $e->timesheet ?: (new Timesheet());
                        /* @var $timesheet Timesheet */
                        $event = $e->event;
                        /* @var $event \common\models\Event */
                        $shiftPosition = $e->shiftPosition;
                        /* @var $shiftPosition ShiftPosition */
                        $shift = $shiftPosition->shift;
                        /* @var $shift Shift */
                        $client = $event->client;
                        /* @var $client \common\models\Client */
                        $employee = $e->employee;

                        $timesheetAnalyzer = $timesheet->analyze();
                        /* @var $timesheetAnalyzer BaseTimesheetAnalyzer */
                        $penaltyCalculator = $timesheetAnalyzer->getPenaltyCalculator();
                        /* @var $penaltyCalculator \common\components\payroll\BasePenaltyCalculator */

                        list($shiftStartTime, $shiftEndTime) = [new \DateTime($shift->start), new \DateTime($shift->end)];
                        /* @var $shiftStartTime \DateTime */
                        /* @var $shiftEndTime \DateTime */

                        $minBilling = $e->getMinBillingHours();

                        $purchaseOrder = ArrayHelper::getValue(ClientPurchaseOrder::findOneByEvent($e->event), 'value');
                        // check if event has #po
                        if ($purchaseOrder) {
                            $po[] = $purchaseOrder;
                        }

                        if (!empty($e->event->client->wcCode->wc_code)) {
                            $wc_code = $e->event->client->wcCode->wc_code;
                            $wc_rate = $e->event->client->wcCode->rate;
                        } else {
                            $wc_code = '';
                            $wc_rate = '';
                        }

                        $clientMsp = $e->event->client->msp;
                        $mspRate = $clientMsp ? $clientMsp->rate : '';

                        $venue_service_charge = (!empty($e->event->venue->service_charge) ? $e->event->venue->service_charge : 0);

                        $pay_rate = $e->rate;
                        $bill_rate = $e->bill_rate;

                        // get venue position rate
                        $venue_position_rate = 0;
                        foreach ($e->event->venue->positions as $pos) {
                            /* @var $pos \common\models\VenuePosition */
                            
                            if ($pos->position_id == $e->shiftPosition->position_id) {
                                $venue_position_rate = $pos->getAmounts($event->date)->pay_rate;
                            }
                        }

                        $ot_bill_rate = $bill_rate * $client->getOtMultiplier($event->date, 1.5);
                        $dt_bill_rate = $e->bill_rate * $client->getDtMultiplier($event->date, 2);

                        $employee_parking = !(empty($e->timesheet->employee_parking)) ? $e->timesheet->employee_parking : 0;
                        $employee_travel = !(empty($e->timesheet->employee_travel)) ? $e->timesheet->employee_travel : 0;
                        $employee_tips = !(empty($e->timesheet->employee_tips)) ? $e->timesheet->employee_tips : 0;
                        $employee_service_charge = !(empty($e->timesheet->employee_service_charge)) ? $e->timesheet->employee_service_charge : 0;
                        $client_parking = !(empty($e->timesheet->client_parking)) ? $e->timesheet->client_parking : 0;
                        $client_travel = !(empty($e->timesheet->client_travel)) ? $e->timesheet->client_travel : 0;
                        $client_tips = !(empty($e->timesheet->client_tips)) ? $e->timesheet->client_tips : 0;

                        $client_service_charge = !(empty($e->timesheet->client_service_charge)) ? $e->timesheet->client_service_charge : 0;

                        $client_service_amount = (($bill_rate * $client_service_charge) / 100) + ($venue_service_charge);
                        $employee_service_amount = ($pay_rate * $employee_service_charge) / 100;

                        $emp_meal_penalty_hours = !(empty($e->timesheet->employee_no_break_penalty)) ? $e->timesheet->employee_no_break_penalty : 0;
                        $client_meal_penalty_hours = !(empty($e->timesheet->client_no_break_penalty)) ? $e->timesheet->client_no_break_penalty : 0;

                        if (
                                ! $timesheetAnalyzer->getWorkedByClient()
                                || ! $timesheetAnalyzer->getWorkedByEmployee()
                                ) {
                            $bonus_pay = 0;
                            $additional_shift_pay = 0;

                            $employee_tips = 0;
                            $client_tips = 0;

                            $employee_service_charge = 0;
                            $client_service_charge = 0;

                            $employee_service_amount = 0;
                            $client_service_amount = 0;

                            $emp_meal_penalty_hours = 0;
                            $client_meal_penalty_hours = 0;

                            $client_travel = 0;
                            $client_parking = 0;

                            $employee_travel = 0;
                            $employee_parking = 0;
                        } else {
                            $bonus_pay = $e->shiftPosition->bonus;
                            $additional_shift_pay = \common\models\AdditionalShiftPay::getSumBy($e->event->date);
                        }

                        if (
                            (
                                PayrollExportForm::TYPE_TIMESHEET_VERIFICATION == $type
                                || PayrollExportForm::TYPE_TIMESHEET_VERIFICATION_CLIENTS == $type
                            )
                        ) {
                            if (
                                $shift->start > date('Y-m-d H:i:s')
                                || (
                                    !$timesheetAnalyzer->getFilledByClient()
                                    && !$timesheetAnalyzer->getFilledByEmployee()
                                )
                            ) {
                                $seconds = $shift->getSeconds();
                            } else {
                                try {
                                    $seconds = $timesheetAnalyzer->getSeconds();
                                } catch (TimesheetException $ex) {
                                    $seconds = $timesheetAnalyzer->getFilledByEmployee()
                                        ? $timesheetAnalyzer->getSecondsByEmployee(false)
                                        : 0;
                                }
                            }
                        } else {
                            try {
                                $seconds = $timesheetAnalyzer->getSeconds(false);
                            } catch (TimesheetException $ex) {
                                $seconds = 0;
                            }
                        }

                        $reg_hours = 0;

                        // if set min pay so employee amount will be change.
                        if (
                                $timesheet->employee_min_pay
                                || (
                                    Timesheet::WORKED_SENTHOME == $timesheet->employee_worked
                                    && USStates::CALIFORNIA == $event->state
                                )
                                ) {
                            $reg_hours = $shift->getMinBillingHours();
                            $workedHours = $seconds / 3600;

                            try {
                                $lateHours = $timesheetAnalyzer->calculateLateSeconds() / 3600;
                            } catch (TimesheetException $ex) {
                                $lateHours = 0;
                            }

                            if ($lateHours && $workedHours < $reg_hours) {
                                $reg_hours -= $lateHours;
                            } elseif ($workedHours > $reg_hours) {
                                $reg_hours = $workedHours;
                            }

                            $reg_hours = max($reg_hours, 2);
                            $reg_hours = min($reg_hours, 4);
                        } else {
                            $reg_hours = $seconds / 3600;
                        }

                        if (
                                in_array($timesheet->employee_worked, [
                                    Timesheet::WORKED_SENTHOME,
                                    Timesheet::WORKED_CANCELLED,
                                ])
                                ) {
                            $employeeCalcWorkedHours = $seconds / 3600;

                            $employeeNonWorkedHours
                                = $reg_hours >= $employeeCalcWorkedHours
                                    ? $reg_hours - $employeeCalcWorkedHours
                                    : 0;
                        } else {
                            $employeeNonWorkedHours = 0;
                        }

                        if ($employeeNonWorkedHours) {
                            $reg_hours -= $employeeNonWorkedHours;
                        }

                        // calculate OT hours
                        $ot_hours = 0;
                        $dt_hours = 0;
                        $reg_sal_hours = $reg_hours;

                        if (in_array($event->state, [USStates::CALIFORNIA, USStates::NEVADA])) {
                            // get 8 reg hours
                            if ($reg_hours > 8) {
                                $reg_sal_hours = 8;
                            } else {
                                $reg_sal_hours = $reg_hours;
                            }

                            if (USStates::CALIFORNIA == $event->state) {
                                if (8 < $reg_hours && 12 > $reg_hours) {
                                    $ot_hours = $reg_hours - 8;
                                } elseif (8 < $reg_hours) {
                                    $ot_hours = 4;
                                }
                            }

                            if (USStates::NEVADA == $event->state) {
                                if ($reg_hours > 8) {
                                    $ot_hours = $reg_hours - 8;
                                }
                            }

                            // calculate DT hours
                            if (USStates::CALIFORNIA == $event->state && $reg_hours > 12) {
                                $dt_hours = $reg_hours - 12;
                            }
                        }

                        // calculate OT and DT Rates
                        $ot_rate = $pay_rate * 1.5;
                        $dt_rate = $pay_rate * 2;

                        // calculate total Tip
                        //$tip = ( ($pay_rate * $reg_sal_hours) + ($pay_rate*1.5*$ot_hours) + ($pay_rate*2*$dt_hours) );
                        // calculate regular , ot, dt salary
                        $reg_salary = $reg_sal_hours * $pay_rate;
                        $ot_salary = $ot_hours * $ot_rate;
                        $dt_salary = $dt_hours * $dt_rate;
                        $nonWorkedSalary = $employeeNonWorkedHours * $pay_rate;

                        // calculate billing amount

                        if ($timesheet->client_min_bill) {
                            // if chose minimum bill on client side so only client side will change.
                            $client_reg_hours = $shift->getMinBillingHours();
                            $workedHours = $timesheetAnalyzer->getSecondsByClient(false) / 3600;
                            $lateHours = $timesheetAnalyzer->calculateClientLateSeconds(false) / 3600;

                            if ($lateHours && $workedHours < $client_reg_hours) {
                                $client_reg_hours -= $lateHours;
                            } elseif ($workedHours > $client_reg_hours) {
                                $client_reg_hours = $workedHours;
                            }
                        } else {
                            $client_reg_hours = $seconds / 3600; // default hours
                        }

                        if (
                                in_array($timesheet->employee_worked, [
                                    Timesheet::WORKED_SENTHOME,
                                    Timesheet::WORKED_CANCELLED,
                                ])
                                ) {
                            $clientCalcWorkedHours = $seconds / 3600;

                            $clientNonWorkedHours
                                = $client_reg_hours >= $clientCalcWorkedHours
                                    ? $client_reg_hours - $clientCalcWorkedHours
                                    : 0;
                        } else {
                            $clientNonWorkedHours = 0;
                        }

                        if ($clientNonWorkedHours) {
                            $client_reg_hours -= $clientNonWorkedHours;
                        }

                        // get 8 reg hours
                        if ($client_reg_hours > 8) {
                            $client_reg_sal_hours = 8;
                        } else {
                            $client_reg_sal_hours = $client_reg_hours;
                        }

                        // calculate OT hours
                        if (8 < $client_reg_hours && 12 > $client_reg_hours) {
                            $client_ot_hours = $client_reg_hours - 8;
                        } elseif (8 < $client_reg_hours) {
                            $client_ot_hours = 4;
                        } else {
                            $client_ot_hours = 0;
                        }

                        // calculate DT hours
                        if ($client_reg_hours > 12) {
                            $client_dt_hours = $client_reg_hours - 12;
                        } else {
                            $client_dt_hours = 0;
                        }

                        // if set no ot

                        $ot_bill_amount = $ot_bill_rate * $client_ot_hours;
                        $dt_bill_amount = $dt_bill_rate * $client_dt_hours;

                        $reg_bill_amount = $bill_rate * $client_reg_sal_hours;
                        $nonWorkedbillAmount = $bill_rate * $clientNonWorkedHours;

                        // if set no bill

                        if ($timesheet->client_no_bill) {
                            $reg_bill_amount = 0;
                            $ot_bill_amount = 0;
                            $dt_bill_amount = 0;
                            $nonWorkedbillAmount = 0;
                        }

                        // if set no pay
                        if ($timesheet->employee_no_pay == 1) {
                            $reg_salary = 0;
                            $ot_salary = 0;
                            $dt_salary = 0;
                            $nonWorkedSalary = 0;
                        }

                        $travelPay = 0;
                        $travelBill = 0;

                        if ($timesheet->employee_travel_distance) {
                            $travelAmounts = TravelRate::getAmounts($event->date);

                            $travelBill = $timesheet->employee_travel_distance * $travelAmounts->bill_rate;
                            $travelPay = $timesheet->employee_travel_distance * $travelAmounts->pay_rate;
                        }

                        if ($emp_meal_penalty_hours) {
                            $mealPenaltyPayAmounts = $penaltyCalculator->getPayAmount(false);
                            /* @var $mealPenaltyPayAmounts \common\components\payroll\PenaltyAmounts */

                            $meal1PenaltyPayAmount = $mealPenaltyPayAmounts->get(BaseMealBreakPolicy::BREAK_INDEX_1);
                            $meal2PenaltyPayAmount = $mealPenaltyPayAmounts->get(BaseMealBreakPolicy::BREAK_INDEX_2);
                        } else {
                            $meal1PenaltyPayAmount = 0;
                            $meal2PenaltyPayAmount = 0;
                        }
                        
                        $employee_meal_penalty_amount = $meal1PenaltyPayAmount + $meal2PenaltyPayAmount;
                        
                        if ($client_meal_penalty_hours) {
                            $client_meal_penalty_amount = $penaltyCalculator->getBillAmount();
                        } else {
                            $client_meal_penalty_amount = 0;
                        }

                        $gross_pay
                            = (
                                $reg_salary
                                + $ot_salary
                                + $dt_salary
                                + $nonWorkedSalary
                                + $employee_parking
                                + $employee_travel
                                + $employee_tips
                                + $employee_service_amount
                                + $employee_meal_penalty_amount
                                + $bonus_pay
                                + $additional_shift_pay
                            );

                        $total_billing_amount
                            = (
                                $reg_bill_amount
                                + $ot_bill_amount
                                + $dt_bill_amount
                                + $nonWorkedbillAmount
                                + $client_parking
                                + $client_travel
                                + ($client_tips * $tipsMultiplier)
                                + $client_service_amount
                                + $client_meal_penalty_amount
                                + $travelBill
                            );

                        $paw = $total_billing_amount - $gross_pay;

                        $sheet->setCellValue($columnNamesByHeader['Day'] . $index, date('l', strtotime($e->event->date)));
                        $sheet->setCellValue($columnNamesByHeader['Date'] . $index, date("m/d/Y", strtotime($e->event->date)));
                        $sheet->setCellValue($columnNamesByHeader['WC'] . $index, $wc_code);
                        $sheet->setCellValue(
                            $columnNamesByHeader['Client'] . $index,
                            join(' - ', array_filter([
                                ArrayHelper::getValue($e->event->client, 'msp.name'),
                                ArrayHelper::getValue($e->event->client, 'division.name'),
                                $e->event->client->name,
                            ]))
                        );

                        $sheet->setCellValue(
                            $columnNamesByHeader['Markup'] . $index,
                            $e->event->client->markup
                        );

                        if (isset($columnNamesByHeader['Event State'])) {
                            $sheet->setCellValue(
                                $columnNamesByHeader['Event State'] . $index,
                                ArrayHelper::getValue($usStateLabels, $event->state, '')
                            );
                        }

                        $sheet->setCellValue(
                            $columnNamesByHeader[$venueColumn] . $index,
                            ArrayHelper::getValue($e->event->venue, ($isAllEmployeesReport ? 'venue_code' : 'name'))
                        );
                        $sheet->setCellValue($columnNamesByHeader['Event'] . $index, $e->event->title);
                        $sheet->setCellValue(
                            $columnNamesByHeader['Position'] . $index,
                            ShiftPosition::getDescriptionBy($e->shiftPosition, false)
                        );
                        $sheet->setCellValue(
                            $columnNamesByHeader['Code'] . $index,
                            $e->shiftPosition->code
                        );

                        if (isset($columnNamesByHeader['AM/PM'])) {
                            $sheet->setCellValue(
                                $columnNamesByHeader['AM/PM'] . $index,
                                date('A', strtotime($shift->start))
                            );
                        }

                        $sheet->setCellValue($columnNamesByHeader['#Emp'] . $index, $employee->payroll_id);
                        $sheet->setCellValue($columnNamesByHeader['First Name'] . $index, strtoupper($employee->first_name));
                        $sheet->setCellValue($columnNamesByHeader['Last Name'] . $index, strtoupper($employee->last_name));

                        if (
                            PayrollExportForm::TYPE_TIMESHEET_VERIFICATION != $type
                            && PayrollExportForm::TYPE_TIMESHEET_VERIFICATION_CLIENTS != $type
                        ) {
                            $sheet->setCellValue($columnNamesByHeader['Reg H (e)'] . $index, $this->number_format($reg_sal_hours));
                            $sheet->setCellValue($columnNamesByHeader['OT H (e)'] . $index, $this->number_format($ot_hours));
                            $sheet->setCellValue($columnNamesByHeader['DT H (e)'] . $index, $this->number_format($dt_hours));
                            $sheet->setCellValue($columnNamesByHeader['Meal H (e)'] . $index, $emp_meal_penalty_hours ?: '');
                            $sheet->setCellValue(
                                $columnNamesByHeader['Non-Worked Hours (e)'] . $index,
                                $this->number_format($employeeNonWorkedHours)
                            );
                            $sheet->setCellValue($columnNamesByHeader['Pay Rate'] . $index, $pay_rate);
                            $sheet->setCellValue($columnNamesByHeader['OT Rate'] . $index, "=SUM({$columnNamesByHeader['Pay Rate']}{$index})*1.5");
                            $sheet->setCellValue($columnNamesByHeader['DT Rate'] . $index, "=SUM({$columnNamesByHeader['Pay Rate']}{$index})*2");
                            $sheet->setCellValue($columnNamesByHeader['Std Pay'] . $index, $reg_salary);
                            $sheet->setCellValue($columnNamesByHeader['OT P'] . $index, $ot_salary);
                            $sheet->setCellValue($columnNamesByHeader['DT P'] . $index, $dt_salary);
                            $sheet->setCellValue($columnNamesByHeader['Bonus'] . $index, $this->number_format($bonus_pay));
                            $sheet->setCellValue($columnNamesByHeader['Additional Shift Pay'] . $index, $this->number_format($additional_shift_pay));
                            $sheet->setCellValue($columnNamesByHeader['Tip (e)'] . $index, $this->number_format($employee_tips));
                            $sheet->setCellValue($columnNamesByHeader['Park (e)'] . $index, $this->number_format($employee_parking));
                            $sheet->setCellValue($columnNamesByHeader['Travel (e)'] . $index, $this->number_format($employee_travel));
                            $sheet->setCellValue($columnNamesByHeader['Service (e)'] . $index, $this->number_format($employee_service_amount));
                            
                            $sheet->setCellValue(
                                $columnNamesByHeader['Meal 1 (e)'] . $index,
                                $meal1PenaltyPayAmount
                            );
                            $sheet->setCellValue(
                                $columnNamesByHeader['Meal 2 (e)'] . $index,
                                $meal2PenaltyPayAmount
                            );
                            $sheet->setCellValue(
                                $columnNamesByHeader['Non-Worked Pay (e)'] . $index,
                                $this->number_format($employeeNonWorkedHours * $pay_rate)
                            );
                            $sheet->setCellValue(
                                $columnNamesByHeader['Gross Pay'] . $index,
                                "=SUM({$columnNamesByHeader['Std Pay']}{$index}:{$columnNamesByHeader['Non-Worked Pay (e)']}{$index})"
                            );
                            $sheet->setCellValue($columnNamesByHeader['P.A.W'] . $index, $this->number_format($paw));
                        }

                        $sheet->setCellValue($columnNamesByHeader['Bill Rate'] . $index, $this->number_format($bill_rate));
                        $sheet->setCellValue($columnNamesByHeader['Reg H (c)'] . $index, $this->number_format($client_reg_sal_hours));
                        $sheet->setCellValue($columnNamesByHeader['OT H (c)'] . $index, $this->number_format($client_ot_hours));
                        $sheet->setCellValue($columnNamesByHeader['DT H (c)'] . $index, $this->number_format($client_dt_hours));
                        $sheet->setCellValue(
                            $columnNamesByHeader['Non-Worked Hours (c)'] . $index,
                            $this->number_format($clientNonWorkedHours)
                        );
                        $sheet->setCellValue($columnNamesByHeader['OT R'] . $index, $this->number_format($ot_bill_rate));
                        $sheet->setCellValue($columnNamesByHeader['DT R'] . $index, $this->number_format($dt_bill_rate));
                        $sheet->setCellValue($columnNamesByHeader['Tip (c)'] . $index, $this->number_format($client_tips));
                        $sheet->setCellValue($columnNamesByHeader['Park (c)'] . $index, $this->number_format($client_parking));
                        $sheet->setCellValue($columnNamesByHeader['Travel (c)'] . $index, $this->number_format($client_travel));
                        $sheet->setCellValue($columnNamesByHeader['Service (c)'] . $index, $this->number_format($client_service_amount));

                        $sheet->setCellValue($columnNamesByHeader['Meal (c)'] . $index, $client_meal_penalty_hours);

                        $sheet->setCellValue(
                            $columnNamesByHeader['Non-Worked Bill (c)'] . $index,
                            $this->number_format($nonWorkedbillAmount)
                        );
                        $sheet->setCellValue($columnNamesByHeader['Total Bill'] . $index, $this->number_format($total_billing_amount));

                        $sheet->setCellValue(
                            $columnNamesByHeader['Status'] . $index,
                            ArrayHelper::getValue(Timesheet::getWorkedLabels(), $timesheet->client_worked)
                        );

                        $sheet->setCellValue(
                            $columnNamesByHeader['Work State'] . $index,
                            $employee->minWage ? $employee->minWage->description : ''
                        );

                        $sheet->setCellValue($columnNamesByHeader['Employee Notes'] . $index, $timesheet->employee_notes);
                        $sheet->setCellValue($columnNamesByHeader['Client Notes'] . $index, $timesheet->client_notes);

                        if (
                            PayrollExportForm::TYPE_TIMESHEET_VERIFICATION == $type
                            || PayrollExportForm::TYPE_TIMESHEET_VERIFICATION_CLIENTS == $type
                        ) {
                            $cVerification = '';
                            $eVerification = '';

                            if (! $e->cancel_reason) {
                                if ($shift->end > date('Y-m-d H:i:s')) {
                                    $cVerification = 'Scheduled';
                                    $eVerification = 'Scheduled';
                                } elseif (! in_array($event->timeclock, Event::getVerificationTimeclockModes())) {
                                    $cVerification = 'Non-ETK';
                                    $eVerification = 'Non-ETK';
                                } else {
                                    $cVerification
                                        = in_array($timesheet->start_verified, Timesheet::getIsVerifiedCases())
                                        && in_array($timesheet->end_verified, Timesheet::getIsVerifiedCases())
                                            ? 'Verified'
                                            : 'Unverified';

                                    if ($timesheet->start_verified_at && $timesheet->end_verified_at) {
                                        $eVerification = 'Code Used';
                                    } elseif ($timesheet->start_verified_at) {
                                        $eVerification = 'Code Used (start)';
                                    } elseif ($timesheet->end_verified_at) {
                                        $eVerification = 'Code Used (end)';
                                    } else {
                                        $eVerification = 'No code used';
                                    }
                                }
                            }

                            $sheet->setCellValue($columnNamesByHeader['Verification (c)'] . $index, $cVerification);
                            $sheet->setCellValue($columnNamesByHeader['Verification (e)'] . $index, $eVerification);
                            $sheet->setCellValue($columnNamesByHeader['Cancellation Reason'] . $index, ArrayHelper::getValue(ShiftEmployee::getCancelReasonLabels(), $e->cancel_reason));
                        } else {
                            $sheet->setCellValue($columnNamesByHeader['Reg B'] . $index, $this->number_format($reg_bill_amount));

                            $sheet->setCellValue(
                                $columnNamesByHeader['Notes'] . $index,
                                join('|', array_map(
                                    function ($note) {
                                        return strip_tags($note);
                                    },
                                    \common\models\EmployeeNote::getNotesForReport($e->shift_employee_id)
                                ))
                            );

                            $reconciled = '';

                            if (!empty($timesheet->use_sheet)) {
                                if (Timesheet::USE_SHEET_CLIENT == $timesheet->use_sheet) {
                                    $reconciled = 'Client';
                                } elseif (Timesheet::USE_SHEET_EMPLOYEE == $timesheet->use_sheet) {
                                    $reconciled = 'Employee';
                                }
                            }

                            $employeeMealReasonLabels = $timesheet->getDictionary(\common\dictionaries\timesheet\mealreason\EmployeeMealReason::class)->getLabels();
                            $clientMealReasonLabels = $timesheet->getDictionary(\common\dictionaries\timesheet\mealreason\ClientMealReason::class)->getLabels();

                            $sheet->setCellValue(
                                $columnNamesByHeader['Meal Break Minutes (e)'] . $index,
                                ($timesheetAnalyzer->calculateEmployeeBreakSeconds() / 60)
                            );
                            $sheet->setCellValue(
                                $columnNamesByHeader['Meal Break Reason (e)'] . $index,
                                ArrayHelper::getValue($employeeMealReasonLabels, $timesheet->employee_no_break_reason, '')
                            );
                            $sheet->setCellValue(
                                $columnNamesByHeader['2nd Meal Break Reason (e)'] . $index,
                                ArrayHelper::getValue($employeeMealReasonLabels, $timesheet->employee_no_sec_break_reason, '')
                            );
                            $sheet->setCellValue(
                                $columnNamesByHeader['Meal Break Minutes (c)'] . $index,
                                ($timesheetAnalyzer->calculateClientBreakSeconds() / 60)
                            );
                            $sheet->setCellValue(
                                $columnNamesByHeader['Meal Break Reason (c)'] . $index,
                                ArrayHelper::getValue($clientMealReasonLabels, $timesheet->client_no_break_reason, '')
                            );
                            $sheet->setCellValue(
                                $columnNamesByHeader['2nd Meal Break Reason (c)'] . $index,
                                ArrayHelper::getValue($clientMealReasonLabels, $timesheet->client_no_sec_break_reason, '')
                            );
                            $sheet->setCellValue($columnNamesByHeader['Travel Mileage Pay'] . $index, $this->number_format($travelPay));
                            $sheet->setCellValue($columnNamesByHeader['Travel Mileage Bill'] . $index, $this->number_format($travelBill));
                            $sheet->setCellValue($columnNamesByHeader['Reconciled'] . $index, $reconciled);
                        }

                        if ($isAllEmployeesReport) {
                            $sheet->setCellValue($columnNamesByHeader['Position Level'] . $index, ArrayHelper::getValue($e, 'shiftPosition.position.employeePosition.level'));
                            $sheet->setCellValue($columnNamesByHeader['Start Date'] . $index, $e->employee->start_date);
                            $sheet->setCellValue($columnNamesByHeader['Employee Rating'] . $index, $e->timesheet->employee_rating);
                            $sheet->setCellValue($columnNamesByHeader['Client Rating'] . $index, $e->timesheet->client_rating);
                            $sheet->setCellValue($columnNamesByHeader['WC rate'] . $index, $wc_rate);
                            $sheet->setCellValue($columnNamesByHeader['MSP rate'] . $index, $mspRate);

                            $sheet->setCellValue(
                                $columnNamesByHeader['Staffing Manager'] . $index,
                                (
                                empty($e->event->venue->staffing_manager_id)
                                    ? ''
                                    : \common\models\User::getFullnameBy($e->event->venue->staffingmanager)
                                )
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Sales Executive'] . $index,
                                (
                                empty($e->event->client->sales_executive_id)
                                    ? ''
                                    : \common\models\User::getFullnameBy($e->event->client->salesExecutive)
                                )
                            );

                            $firstEvent = $e->event->venue->getFirstEvent();
                            $venueCounty = $e->event->venue->county;

                            $sheet->setCellValue($columnNamesByHeader['County of Venue'] . $index, $venueCounty ? $venueCounty->name : '');
                            $sheet->setCellValue($columnNamesByHeader['Venue Name'] . $index, $e->event->venue->name);
                            $sheet->setCellValue($columnNamesByHeader['1st Event Date'] . $index, $firstEvent ? date("m/d/Y", strtotime($firstEvent->date)) : '');

                            $sheet->setCellValue($columnNamesByHeader['Industry'] . $index, $e->event->client->industry);

                            $sheet->setCellValue($columnNamesByHeader['Rehire Date'] . $index, $e->employee->start_date2 ? date("m/d/Y", strtotime($e->employee->start_date2)) : '');

                            $sheet->setCellValue($columnNamesByHeader['Recruited By'] . $index, $e->employee->recruitedBy ? $e->employee->recruitedBy->getFullname() : '');

                            $sheet->setCellValue($columnNamesByHeader['Cancellation Reason'] . $index, ArrayHelper::getValue(ShiftEmployee::getCancelReasonLabels(), $e->cancel_reason));

                            $surchargeShouldHaveApply
                                = $client->surcharge_deadline
                                    && $shiftPosition->created_at <= $shiftStartTime->format('Y-m-d H:i:s')
                                    && ($shiftStartTime->format('U') - $shiftStartTime->format('U')) / 3600 <= $client->surcharge_deadline;

                            $sheet->setCellValue(
                                $columnNamesByHeader['Surcharge Billed'] . $index,
                                (
                                    $surchargeShouldHaveApply
                                        ? ($shiftPosition->surcharge ? 'Yes' : 'No')
                                        : 'N/A'
                                )
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Minimum Billed'] . $index,
                                ($e->timesheet->client_min_bill ? 'Yes' : 'No')
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Cancelled In Deadline'] . $index,
                                ($e->cancelled_in_deadline ? 'Yes' : 'No')
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Background'] . $index,
                                ArrayHelper::getValue(Employee::getBackgroundLabels(), $e->employee->background)
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Background Query'] . $index,
                                ArrayHelper::getValue(Employee::getBackgroundQueryLabels(), $e->employee->background_query)
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Client Won Date'] . $index,
                                $e->event->client->won_date ? date("m/d/Y", strtotime($e->event->client->won_date)) : ''
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Prior Won Date'] . $index,
                                $this->getClientPriorWonDate($client)
                            );
                        }

                        $sheet->getStyle("A{$index}:{$lastColumnName}{$index}")->applyFromArray($style);
                        $sheet->getRowDimension($index)->setRowHeight(25);

                        if ($timesheetAnalyzer->getDiscrepancy()) {
                            $spreadsheet
                                ->getActiveSheet()
                                ->getStyle("A{$index}:{$lastColumnName}{$index}")
                                ->getFill()
                                ->setFillType(\PhpOffice\PhpSpreadsheet\Style\Fill::FILL_SOLID)
                                ->getStartColor()
                                ->setARGB('ff2222');
                        } elseif ($index % 2 == 0) {
                            $spreadsheet
                                ->getActiveSheet()
                                ->getStyle("A{$index}:{$lastColumnName}{$index}")
                                ->getFill()
                                ->setFillType(\PhpOffice\PhpSpreadsheet\Style\Fill::FILL_SOLID)
                                ->getStartColor()
                                ->setARGB('D3D3D3');
                        }

                        if (
                            PayrollExportForm::TYPE_TIMESHEET_VERIFICATION == $type
                            || PayrollExportForm::TYPE_TIMESHEET_VERIFICATION_CLIENTS == $type
                        ) {
                            if ($venue_position_rate != $e->rate) {
                                $sheet->getStyle($columnNamesByHeader['Bill Rate'] . $index)->getFill()
                                    ->setFillType(\PhpOffice\PhpSpreadsheet\Style\Fill::FILL_SOLID)
                                    ->getStartColor()->setARGB('95b3d7');
                            }
                        } else {
                            if ($venue_position_rate != $e->rate) {
                                $sheet->getStyle($columnNamesByHeader['Pay Rate'] . $index)->getFill()
                                    ->setFillType(\PhpOffice\PhpSpreadsheet\Style\Fill::FILL_SOLID)
                                    ->getStartColor()->setARGB('95b3d7');
                                $sheet->getStyle($columnNamesByHeader['Bill Rate'] . $index)->getFill()
                                    ->setFillType(\PhpOffice\PhpSpreadsheet\Style\Fill::FILL_SOLID)
                                    ->getStartColor()->setARGB('95b3d7');
                            }

                            // if employee hours used(orange).

                            if ($e->timesheet->use_sheet == Timesheet::USE_SHEET_EMPLOYEE) {
                                $sheet->getStyle($columnNamesByHeader['Reg H (e)'] . $index)
                                    ->getFill()
                                    ->setFillType(\PhpOffice\PhpSpreadsheet\Style\Fill::FILL_SOLID)
                                    ->getStartColor()
                                    ->setARGB('fac090');
                            }

                            // if manager approve OT and select 'Absorbed by Agency'.

                            if ($e->overtime_type == ShiftEmployee::OVERTIME_PAID_BY_AGENCY) {
                                $sheet->getStyle($columnNamesByHeader['Reg H (e)'] . $index)->getFill()
                                    ->setFillType(\PhpOffice\PhpSpreadsheet\Style\Fill::FILL_SOLID)
                                    ->getStartColor()->setARGB('BA55D3');
                            }

                            // get shift start time
                            if ($e->timesheet->use_sheet == Timesheet::USE_SHEET_EMPLOYEE) {
                                $shiftStart = $e->timesheet->employee_start;
                            } else {
                                $shiftStart = $e->timesheet->client_start;
                            }

                            // if employee come late (yellow).
                            if (strtotime($shiftStart) > strtotime($e->shiftPosition->shift->start)) {
                                $sheet->getStyle($columnNamesByHeader['Reg H (e)'] . $index)->getFill()
                                    ->setFillType(\PhpOffice\PhpSpreadsheet\Style\Fill::FILL_SOLID)
                                    ->getStartColor()->setARGB('ffff99');
                            }
                        }

                        $sheet->getStyle("A{$index}:{$lastColumnName}{$index}")->getFont()->setSize(12);

                        $index++;

                        $event_name .= $e->event->title;

                        $total_emp_oth += $ot_hours;
                        $total_emp_dth += $dt_hours;
                        $total_emp_ot += $ot_salary;
                        $total_emp_dt += $dt_salary;
                        $total_emp_tip += $employee_tips;
                        $total_emp_travel += $employee_travel;
                        $total_emp_parking += $employee_parking;
                        $total_emp_service += $employee_service_amount;
                        $total_emp_meal_p += $employee_meal_penalty_amount;
                        $total_emp_meal += $emp_meal_penalty_hours;

                        $total_client_oth += $client_ot_hours;
                        $total_client_dth += $client_dt_hours;
                        $total_client_tip += $client_tips;
                        $total_client_travel += $client_travel;
                        $total_client_parking += $client_parking;
                        $total_client_service += $client_service_amount;
                        $total_client_meal_p += $client_meal_penalty_amount;
                        $total_client_meal += $client_meal_penalty_hours;

                        $total_gross_pay += $gross_pay;

                        $total_reg_hours += $reg_sal_hours;
                        $total_ot_hours += $ot_hours;
                        $total_dt_hours += $dt_hours;
                        $total_reg_salary += $reg_salary;
                        $total_ot_salary += $ot_salary;
                        $total_dt_salary += $dt_salary;
                        $totalNonWorkedSalary += $nonWorkedSalary;
                        $grand_bill_amount += $total_billing_amount;
                        $total_paw += $paw;
                    }

                    if ($isAllEmployeesReport) {
                        $employeeOtherWorkQuery = EmployeeOtherWork::find()
                            ->andWhere("date >= '{$this->startDate}'")
                            ->andWhere("date <= '{$this->endDate}'");

                        if (PayrollExportForm::TYPE_EMPLOYEE == $type) {
                            $employeeOtherWorkQuery->andWhere(['employee_id' => $employee_id]);
                        }

                        foreach ($employeeOtherWorkQuery->each() as $employeeOtherWork) {
                            /* @var $employeeOtherWork EmployeeOtherWork */
                            $employee = $employeeOtherWork->employee;
                            /* @var $employee Employee */
                            $otherWorkType = $employeeOtherWork->otherWorkType;
                            /* @var $otherWorkType \common\models\OtherWorkType */

                            $reimbWorkHours = $employeeOtherWork->work_hours;
                            $reimbNonWorkHours = $employeeOtherWork->non_work_hours;
                            $reimbHoursPay = $employeeOtherWork->getTotalPayByHours();
                            $reimbCostPay = $employeeOtherWork->cost;

                            $isCert = Certification::find()
                                ->andWhere(['other_work_type_id' => $employeeOtherWork->other_work_type_id])
                                ->exists();

                            $sheet->getStyle("A{$index}:{$lastColumnName}{$index}")->applyFromArray($style);

                            $sheet->setCellValue(
                                $columnNamesByHeader['Day'] . $index,
                                date('l', strtotime($employeeOtherWork->created_at))
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Date'] . $index,
                                date("m/d/Y", strtotime($employeeOtherWork->created_at))
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['WC'] . $index,
                                ((function () use (&$employee) {
                                    $shiftEmployee = Employee::getLastShiftEmployeeBy($employee);
                                    /* @var $shiftEmployee ShiftEmployee|null */
                                    
                                    if ($shiftEmployee) {
                                        $code = \common\helpers\ArrayHelper::getValue($shiftEmployee->event->client, 'wcCode.wc_code');
                                        
                                        if (trim($code)) {
                                            return $code;
                                        }
                                    }
                                    
                                    return "{$employee->state}8810";
                                })())
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader[$venueColumn] . $index,
                                'CSS'
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Event'] . $index,
                                'N/A'
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Position'] . $index,
                                $otherWorkType->name
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['#Emp'] . $index,
                                $employee->payroll_id
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['First Name'] . $index,
                                strtoupper($employee->first_name)
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Last Name'] . $index,
                                strtoupper($employee->last_name)
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Work State'] . $index,
                                $employee->minWage ? $employee->minWage->description : ''
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Reg H (e)'] . $index,
                                $reimbWorkHours
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Std Pay'] . $index,
                                $employeeOtherWork->getPayByWorkHours()
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Non-Worked Hours (e)'] . $index,
                                $reimbNonWorkHours
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Non-Worked Pay (e)'] . $index,
                                $employeeOtherWork->getPayByNonWorkHours()
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Cert Cost (e)'] . $index,
                                $isCert ? $employeeOtherWork->cost : 0
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Pay Rate'] . $index,
                                $employeeOtherWork->rate
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Reimb Pay (e)'] . $index,
                                $reimbCostPay
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Gross Pay'] . $index,
                                $reimbHoursPay + $reimbCostPay
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['P.A.W'] . $index,
                                $reimbHoursPay + $reimbCostPay
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Start Date'] . $index,
                                $employee->start_date
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Staffing Manager'] . $index,
                                'N/A'
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Sales Executive'] . $index,
                                'N/A'
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Venue Name'] . $index,
                                'N/A'
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Status'] . $index,
                                'Worked'
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Surcharge Billed'] . $index,
                                'No'
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Minimum Billed'] . $index,
                                'No'
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Cancelled In Deadline'] . $index,
                                'No'
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Background'] . $index,
                                ArrayHelper::getValue(Employee::getBackgroundLabels(), $employee->background)
                            );

                            $sheet->setCellValue(
                                $columnNamesByHeader['Background Query'] . $index,
                                ArrayHelper::getValue(Employee::getBackgroundQueryLabels(), $employee->background_query)
                            );

                            $index++;
                        }
                    }

                    // hide column if empty.

                    if ($type == PayrollExportForm::TYPE_CLIENTS) {
                        if ($total_emp_oth == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['OT H (e)'])->setVisible(false);
                            $sheet->getColumnDimension($columnNamesByHeader['OT Rate'])->setVisible(false);
                        }

                        if ($total_emp_dth == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['DT H (e)'])->setVisible(false);
                            $sheet->getColumnDimension($columnNamesByHeader['DT Rate'])->setVisible(false);
                        }

                        if ($event_name == '') {
                            $sheet->getColumnDimension($columnNamesByHeader['Event'])->setVisible(false);
                        }

                        if ($total_emp_ot == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['OT P'])->setVisible(false);
                        }

                        if ($total_emp_dt == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['DT P'])->setVisible(false);
                        }

                        if ($total_emp_tip == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['Tip (e)'])->setVisible(false);
                        }

                        if ($total_emp_parking == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['Park (e)'])->setVisible(false);
                        }

                        if ($total_emp_travel == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['Travel (e)'])->setVisible(false);
                        }

                        if ($total_emp_service == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['Service (e)'])->setVisible(false);
                        }

                        if ($total_emp_meal_p == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['Meal 1 (e)'])->setVisible(false);
                            $sheet->getColumnDimension($columnNamesByHeader['Meal 2 (e)'])->setVisible(false);
                        }
                        if ($total_client_tip == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['Tip (c)'])->setVisible(false);
                        }
                        if ($total_client_parking == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['Park (c)'])->setVisible(false);
                        }
                        if ($total_client_travel == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['Travel (c)'])->setVisible(false);
                        }
                        if ($total_client_service == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['Service (c)'])->setVisible(false);
                        }
                        if ($total_client_meal_p == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['Meal (c)'])->setVisible(false);
                        }
                        if ($total_client_oth == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['OT H (c)'])->setVisible(false);
                            $sheet->getColumnDimension($columnNamesByHeader['OT R'])->setVisible(false);
                        }
                        if ($total_client_dth == 0) {
                            $sheet->getColumnDimension($columnNamesByHeader['DT H (c)'])->setVisible(false);
                            $sheet->getColumnDimension($columnNamesByHeader['DT R'])->setVisible(false);
                        }
                    }

                    $sheet->getStyle('A' . $index . ':' . $lastColumnName . $index)->applyFromArray($style);
                    $sheet->getStyle('A' . ($index + 1) . ':' . $lastColumnName . ($index + 1))->applyFromArray($style);
                    $sheet->getStyle('A' . ($index + 2) . ':' . $lastColumnName . ($index + 2))->applyFromArray($style);

                    $newIndex = $index + 2;
                    $sheet->setCellValue('G' . $newIndex, 'Total');

                    if (
                        PayrollExportForm::TYPE_TIMESHEET_VERIFICATION != $type
                        && PayrollExportForm::TYPE_TIMESHEET_VERIFICATION_CLIENTS != $type
                    ) {
                        $sheet->setCellValue($columnNamesByHeader['Reg H (e)'] . $newIndex, "=SUM({$columnNamesByHeader['Reg H (e)']}2:{$columnNamesByHeader['Reg H (e)']}{$index})");
                        $sheet->setCellValue($columnNamesByHeader['OT H (e)'] . $newIndex, "=SUM({$columnNamesByHeader['OT H (e)']}2:{$columnNamesByHeader['OT H (e)']}{$index})");
                        $sheet->setCellValue($columnNamesByHeader['DT H (e)'] . $newIndex, "=SUM({$columnNamesByHeader['DT H (e)']}2:{$columnNamesByHeader['DT H (e)']}{$index})");
                        $sheet->setCellValue($columnNamesByHeader['Std Pay'] . $newIndex, "=SUM({$columnNamesByHeader['Std Pay']}2:{$columnNamesByHeader['Std Pay']}{$index})");
                        $sheet->setCellValue($columnNamesByHeader['OT P'] . $newIndex, "=SUM({$columnNamesByHeader['OT P']}2:{$columnNamesByHeader['OT P']}{$index})");
                        $sheet->setCellValue($columnNamesByHeader['DT P'] . $newIndex, "=SUM({$columnNamesByHeader['DT P']}2:{$columnNamesByHeader['DT P']}{$index})");
                        $sheet->setCellValue($columnNamesByHeader['Bonus'] . $newIndex, "=SUM({$columnNamesByHeader['Bonus']}2:{$columnNamesByHeader['Bonus']}{$index})");
                        $sheet->setCellValue($columnNamesByHeader['Tip (e)'] . $newIndex, "=SUM({$columnNamesByHeader['Tip (e)']}2:{$columnNamesByHeader['Tip (e)']}{$index})");
                        $sheet->setCellValue($columnNamesByHeader['Park (e)'] . $newIndex, "=SUM({$columnNamesByHeader['Park (e)']}2:{$columnNamesByHeader['Park (e)']}{$index})");
                        $sheet->setCellValue($columnNamesByHeader['Travel (e)'] . $newIndex, "=SUM({$columnNamesByHeader['Travel (e)']}2:{$columnNamesByHeader['Travel (e)']}{$index})");
                        $sheet->setCellValue($columnNamesByHeader['Service (e)'] . $newIndex, "=SUM({$columnNamesByHeader['Service (e)']}2:{$columnNamesByHeader['Service (e)']}{$index})");
                        $sheet->setCellValue($columnNamesByHeader['Meal 1 (e)'] . $newIndex, "=SUM({$columnNamesByHeader['Meal 1 (e)']}2:{$columnNamesByHeader['Meal 1 (e)']}{$index})");
                        $sheet->setCellValue($columnNamesByHeader['Meal 2 (e)'] . $newIndex, "=SUM({$columnNamesByHeader['Meal 2 (e)']}2:{$columnNamesByHeader['Meal 2 (e)']}{$index})");
                        $sheet->setCellValue($columnNamesByHeader['Gross Pay'] . $newIndex, "=SUM({$columnNamesByHeader['Gross Pay']}2:{$columnNamesByHeader['Gross Pay']}{$index})");
                        $sheet->setCellValue($columnNamesByHeader['P.A.W'] . $newIndex, "=SUM({$columnNamesByHeader['P.A.W']}2:{$columnNamesByHeader['P.A.W']}{$index})");
                    }

                    $sheet->setCellValue($columnNamesByHeader['Total Bill'] . $newIndex, "=SUM({$columnNamesByHeader['Total Bill']}2:{$columnNamesByHeader['Total Bill']}{$index})");
                    $sheet->setCellValue($columnNamesByHeader['Tip (c)'] . $newIndex, "=SUM({$columnNamesByHeader['Tip (c)']}2:{$columnNamesByHeader['Tip (c)']}{$index})");
                    $sheet->setCellValue($columnNamesByHeader['Park (c)'] . $newIndex, "=SUM({$columnNamesByHeader['Park (c)']}2:{$columnNamesByHeader['Park (c)']}{$index})");
                    $sheet->setCellValue($columnNamesByHeader['Travel (c)'] . $newIndex, "=SUM({$columnNamesByHeader['Travel (c)']}2:{$columnNamesByHeader['Travel (c)']}{$index})");
                    $sheet->setCellValue($columnNamesByHeader['Service (c)'] . $newIndex, "=SUM({$columnNamesByHeader['Service (c)']}2:{$columnNamesByHeader['Service (c)']}{$index})");
                    $sheet->setCellValue($columnNamesByHeader['Meal (c)'] . $newIndex, "=SUM({$columnNamesByHeader['Meal (c)']}2:{$columnNamesByHeader['Meal (c)']}{$index})");

                    $sheet
                        ->getStyle("A{$newIndex}:{$lastColumnName}{$newIndex}")
                        ->getFill()
                        ->setFillType(\PhpOffice\PhpSpreadsheet\Style\Fill::FILL_SOLID)
                        ->getStartColor()
                        ->setARGB('FFFF00');

                    $sheet
                        ->getStyle("A{$newIndex}:{$lastColumnName}{$newIndex}")
                        ->getFont()
                        ->setSize(12);

                    $row = $newIndex + 3;
                    $crow = $row + 1;

                    $colorHints = [
                        ['Surcharge/Premium Pay', '95b3d7'],
                        ['Employee Hours Used', 'fac090'],
                        ['Employee Late', 'ffff99'],
                        ['Absorbed by Agency', 'BA55D3'],
                        ['Minimum Bill Applied', '00008B'],
                        ['Minimum Bill Not Applied', '8B0000'],
                        ['Discrepancy', 'ff2222'],
                    ];

                    foreach ($colorHints as $index => $colorHint) {
                        $hRowIndex = $row + $index;

                        $sheet->getStyle('A' . $hRowIndex)
                            ->getFill()
                            ->setFillType(\PhpOffice\PhpSpreadsheet\Style\Fill::FILL_SOLID)
                            ->getStartColor()
                            ->setARGB($colorHint[1]);
                        $sheet->setCellValue('B' . $hRowIndex, $colorHint[0]);
                        $sheet->getStyle('B' . $hRowIndex)->applyFromArray($bold);
                        $sheet->getStyle('B' . $hRowIndex)->getFont()->setSize(12);
                    }

                    if ($type == PayrollExportForm::TYPE_CLIENTS || $type == 2) {
                        if (!empty($minBilling)) {
                            $sheet->setCellValue('G' . $row, 'Min. Billing');
                            $sheet->getStyle('G' . $row)->applyFromArray($bold);
                            $sheet->getStyle('G' . $row)->getFont()->setSize(12);
                            $sheet->setCellValue('G' . $crow, $minBilling . ' Hours');
                            $sheet->getStyle('G' . $crow)->getFont()->setSize(12);
                        }

                        if (!empty($payment_type)) {
                            $sheet->setCellValue('H' . $row, 'Payment Type');
                            $sheet->getStyle('H' . $row)->applyFromArray($bold);
                            $sheet->getStyle('H' . $row)->getFont()->setSize(12);
                            $sheet->setCellValue('H' . $crow, $payment_type);
                            $sheet->getStyle('H' . $crow)->getFont()->setSize(12);
                        }

                        if (!empty($payment_Note)) {
                            $sheet->setCellValue('I' . $row, 'Payment Notes');
                            $sheet->getStyle('I' . $row)->applyFromArray($bold);
                            $sheet->getStyle('I' . $row)->getFont()->setSize(12);
                            $sheet->setCellValue('I' . $crow, $payment_Note);
                            $sheet->getStyle('I' . $crow)->getFont()->setSize(12);
                        }
                    }


                    $prow = $row + 1;
                    $notes = '';
                    foreach ($shift_emp as $key => $value) {
                        foreach ($value->payrollNotes as $key => $payrollNote) {
                            if ($payrollNote->on_sheet == 1) {
                                $sheet->setCellValue('E' . $prow, $payrollNote->note);
                                $sheet->getStyle('E' . $prow)->getFont()->setSize(12);

                                $notes .= $payrollNote->note;
                                $prow += 1;
                                $prow ++;
                            }
                        }
                    }

                    if (!empty($notes)) {
                        $sheet->setCellValue('E' . $row, 'Payroll Notes');
                        $sheet->getStyle('E' . $row)->applyFromArray($bold);
                        $sheet->getStyle('E' . $row)->getFont()->setSize(12);
                    }

                    if ($type == PayrollExportForm::TYPE_CLIENTS && $export_venue_id > 0) {
                        $newRow = $newIndex + 3;
                        $sheet->setCellValue('J' . $newRow, 'Bill/Pay Rates');
                        $sheet->getStyle('J' . $newRow)->applyFromArray($bold);
                        $sheet->getStyle('J' . $newRow)->getFont()->setSize(12);

                        $sheet->setCellValue('K' . $newRow, 'Position');
                        $sheet->getStyle('K' . $newRow)->applyFromArray($bold);
                        $sheet->getStyle('K' . $newRow)->getFont()->setSize(12);

                        $newRow += 1;
                        $i = 1;
                        foreach ($shift_emp as $value) {
                            if ($i == 1) { // get only selected venue position.
                                foreach ($value->event->venue->positions as $position) {
                                    /* @var $position \common\models\VenuePosition */
                                    $venuePositionRateAmounts = $position->getAmounts($event->date);
                                    
                                    $sheet->setCellValue(
                                        'J' . $newRow, 
                                        "{$venuePositionRateAmounts->bill_rate}/{$venuePositionRateAmounts->pay_rate}"
                                    );
                                    
                                    $sheet->getStyle('J' . $newRow)->getFont()->setSize(12);

                                    $sheet->setCellValue('K' . $newRow, strtoupper($position->position->description));
                                    $sheet->getStyle('K' . $newRow)->getFont()->setSize(12);
                                    $newRow++;
                                    $i++;
                                }
                            }
                        }
                    }

                    // show #po if have any
                    if ($po) {
                        $sheet->setCellValue('E' . $row, '#PO');
                        $sheet->getStyle('E' . $row)->applyFromArray($bold);
                        $sheet->getStyle('E' . $row)->getFont()->setSize(12);

                        $p = $row + 1;
                        for ($n = 0; $n < count($po); $n++) {
                            $sheet->setCellValue('E' . $p, $po[$n]);
                            $sheet->getStyle('E' . $p)->getFont()->setSize(12);
                            $p++;
                        }
                    }

                    $fileName
                        = join('-', array_filter([
                            (
                                PayrollExportForm::TYPE_CLIENTS == $this->type
                                || PayrollExportForm::TYPE_TIMESHEET_VERIFICATION_CLIENTS == $this->type
                                    ? (
                                        $export_venue_id
                                            ? Inflector::slug($venue_name)
                                            : Inflector::slug($client_name)
                                    )
                                : 'Payroll'
                            ),
                            date('mdY', strtotime($this->endDate))
                        ])) . '.xlsx';

                    $sheet->setTitle(\yii\helpers\StringHelper::truncate($fileName, 28));

                    $saveLocation = \Yii::getAlias('@api/web') . '/temp/' . $fileName;

                    $writer = new Xlsx($spreadsheet);
                    $writer->save($saveLocation);

                    $spreadsheet->disconnectWorksheets();
                    unset($spreadsheet);

                    return [$saveLocation, $fileName];
                };

        if (is_array($export_venue_id) && 1 < count($export_venue_id)) {
            $zFileName
                = join('-', [
                    'Payroll',
                    date('mdY', strtotime($this->startDate)),
                    date('mdY', strtotime($this->endDate)),
                ]) . '.zip';
            $saveLocation = \Yii::getAlias('@api/web') . '/temp/' . $zFileName;

            if (file_exists($saveLocation)) {
                @unlink($saveLocation); // delete prev. zip
            }

            $zip = new \ZipArchive();
            if ($zip->open($saveLocation, \ZipArchive::CREATE)!==TRUE) {
                exit("cannot open <{$saveLocation}>\n");
            }

            $filePaths = [];
            foreach ($export_venue_id as $one_export_venue_id) {
                if (0 === strpos($one_export_venue_id, 'c')) {
                    list($filePath, $fileName)
                        = $createFile(substr($one_export_venue_id, 1));
                } else {
                    list($filePath, $fileName)
                        = $createFile(
                        $export_client_id,
                        $one_export_venue_id
                    );
                }

                $zip->addFile($filePath, $fileName);

                $filePaths[] = $filePath;
            }
            $zip->close();

            foreach ($filePaths as $filePath) {
                unlink($filePath);
            }

            $jobModel = \common\models\Job::findOne($this->jobId);
            $jobModel->end(['filename' => $zFileName]);

            return $saveLocation;
        }

        $one_export_venue_id
            = is_array($export_venue_id)
            ? $export_venue_id[0]
            : $export_venue_id;

        if (0 === strpos($one_export_venue_id, 'c')) {
            $export_client_id = substr($one_export_venue_id, 1);
            $one_export_venue_id = null;
        }

        list($filePath, $fileName)
            = $createFile(
            $export_client_id,
            $one_export_venue_id,
            $employee_id,
            true
        );

        if ($this->jobId) {
            $jobModel = \common\models\Job::findOne($this->jobId);
            $jobModel->end(['filename' => $fileName]);
        }

        return $filePath;
    }

    private function number_format($value)
    {
        $numeric = $value + 0;

        if (is_int($numeric)) {
            return (string) $numeric; // nincs tizedes
        }

        if (is_float($numeric)) {
            return number_format($numeric, 2, '.', ''); // 2 tizedes, ponttal
        }

        return $value;
    }

    private $_clientsPriorWonDates = [];

    private function getClientPriorWonDate(Client $client)
    {
        if (isset($this->_clientsPriorWonDates[$client->client_id])) {
            return $this->_clientsPriorWonDates[$client->client_id];
        }

        $clientPriorWonDates = [];
        $modelName = Utils::classShortName($client);
        $clientHistoryQuery = HistoryEntry::find()
            ->andWhere(['related_id' => $client->client_id])
            ->andWhere(['related' => $modelName])
            ->andWhere(['model' => $modelName])
            ->andWhere(['LIKE', 'changes', '"won_date"']);

        foreach ($clientHistoryQuery->each() as $historyEntry) {
            /* @var $historyEntry HistoryEntry */

            foreach ($historyEntry->changes as $change) {
                if (
                    empty($change['model'])
                    || $change['model'][0] != $modelName
                    || empty($change['attributes'])
                    || empty($change['attributes']['won_date'])
                    || empty($change['attributes']['won_date']['old'])
                ) {
                    continue;
                }

                $clientPriorWonDates[] = date("m/d/Y", strtotime($change['attributes']['won_date']['old']));
            }
        }

        $this->_clientsPriorWonDates[$client->client_id] = implode(', ', $clientPriorWonDates);

        return $this->_clientsPriorWonDates[$client->client_id];
    }
}