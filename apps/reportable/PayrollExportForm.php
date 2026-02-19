<?php

namespace api\models\form;

use common\models\Job;
use common\models\Msp;
use common\jobs\BenefitSickExportJob;
use Yii;
use yii\helpers\ArrayHelper;
use common\components\UsesActorTrait;
use yii\base\InvalidArgumentException;
use yii\db\Query;
use common\components\Utils;
use common\models\ShiftEmployee;
use common\models\Preference;
use common\models\Employee;
use common\models\Event;
use common\models\Timesheet;
use common\models\SickWageEmployee;
use common\models\DocumentType;
use common\models\EmployeeNote;
use common\models\User;
use PhpOffice\PhpSpreadsheet\Spreadsheet;
use PhpOffice\PhpSpreadsheet\Writer\Xlsx;
use PhpOffice\PhpSpreadsheet\Cell\Coordinate;
use kartik\mpdf\Pdf;
use common\jobs;

/**
 * Description of PayrollExportForm
 *
 * @author gabor
 */
class PayrollExportForm extends \yii\base\Model implements \api\components\FormBaseFieldsInterface
{
    use UsesActorTrait;

    public $type;
    public $client_id;
    public $venue_id;
    public $employee_id;
    public $date_from;
    public $date_to;
    public $invoie_start_no;
    public $client_document_type_id;
    public $employee_document_type_id;
    public $employee_cancel_reason;
    public $tips_multiplier;
    public $msp_id;

    public function rules()
    {
        return [
            [['type', 'client_id', 'venue_id', 'employee_id', 'date_from', 'date_to', 'invoie_start_no', 'employee_cancel_reason', 'msp_id'], 'safe'],
            [['client_document_type_id', 'employee_document_type_id', 'tips_multiplier'], 'integer'],
            [
                'client_document_type_id', 'exist',
                'targetClass' => DocumentType::className(),
                'targetAttribute' => ['client_document_type_id' => 'id'],
                'filter' => ['type' => DocumentType::TYPE_CLIENT_VENUE, 'reportable' => 1],
            ],
            [
                'employee_document_type_id', 'exist',
                'targetClass' => DocumentType::className(),
                'targetAttribute' => ['employee_document_type_id' => 'id'],
                'filter' => ['type' => DocumentType::TYPE_EMPLOYEE, 'reportable' => 1],
            ],
            ['client_document_type_id', 'required', 'when' => function() {
                return $this->type == static::TYPE_CLIENT_AND_VENUE_DOCUMENTS;
            }],
            ['employee_document_type_id', 'required', 'when' => function() {
                return $this->type == static::TYPE_EMPLOYEE_DOCUMENTS;
            }],
        ];
    }

    public function generate()
    {
        if (!$this->validate()) {
            return false;
        }

        $db = Yii::$app->db;
        /* @var $db \yii\db\Connection */
        $formatter = Yii::$app->formatter;
        /* @var $formatter \yii\i18n\Formatter */

        $type = $this->type;
        $start_date = $this->date_from;
        $end_date = $this->date_to;
        $dateRange = "{$this->date_from} - {$this->date_to}";

        $getJobName = function ($type) {
            $data = ArrayHelper::getValue(static::getBaseFields(), 'type.data');

            foreach ($data as $item) {
                if ($item['id'] == $type) {
                    return $item['label'];
                }
            }

            $typeAsString = \yii\helpers\VarDumper::dumpAsString($type);
            throw new InvalidArgumentException("\"{$typeAsString}\" is not valid.");
        };

        if (
                in_array($type, [
                    static::TYPE_CLIENTS,
                    static::TYPE_EMPLOYEE,
                    static::TYPE_ALL_CLIENTS_EMPLOYEES,
                    static::TYPE_TIMESHEET_VERIFICATION,
                    static::TYPE_TIMESHEET_VERIFICATION_CLIENTS,
                ])
                ) {
            return Job::enqueue($getJobName($type), [
                \common\jobs\ClientsAndEmployeesExportJob::class,
                [
                    'type' => $type,
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                    'clientId' => $this->client_id,
                    'venueId' => $this->venue_id,
                    'employeeId' => $this->employee_id,
                    'tipsMultiplier' => $this->tips_multiplier,
                    'mspId' => $this->msp_id,
                ]
            ]);

        } elseif (
                in_array($type, [
                    static::TYPE_ALL_CLIENTS_EMPLOYEES_NEW
                ])
        ) {
            return Job::enqueue($getJobName($type), [
                \common\jobs\ClientsAndEmployeesNewExportJob::class,
                [
                    'type' => $type,
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                    'clientId' => $this->client_id,
                    'venueId' => $this->venue_id,
                    'employeeId' => $this->employee_id,
                    'tipsMultiplier' => $this->tips_multiplier,
                ]
            ]);
        } elseif (
                in_array($type, [
                    static::TYPE_SUMMARY,
                    static::TYPE_SUMMARY_OFFSET,
                    static::TYPE_SUMMARY_OFFSET_SCHEDULED
                ])
                ) {
            return Job::enqueue($getJobName($type), [
                \common\jobs\PayrollSummaryExportJob::class,
                [
                    'type' => $type,
                    'start_date' => $start_date,
                    'end_date' => $end_date,
                    'dateRange' => $dateRange,
                ]
            ]);
        } elseif (
                in_array($type, [
                    static::TYPE_SUMMARY_NEW,
                ])
                ) {
            return Job::enqueue($getJobName($type), [
                \common\jobs\PayrollSummaryNewExportJob::class,
                [
                    'type' => $type,
                    'start_date' => $start_date,
                    'end_date' => $end_date,
                    'dateRange' => $dateRange,
                ]
            ]);
        } elseif ($type == 7) { // Paycheck Mailing List
            $filename = 'Paycheck-Mailing-List' . Date('YmdGis') . '.pdf';

            // get all employee event detail with above date range.
            $employees
                = ShiftEmployee::find()
                    ->select([
                        'shift_employee.employee_id',
                        'employee.first_name',
                        'employee.last_name',
                        'employee.payroll_id',
                        'employee.status'
                    ])
                    ->distinct()
                    ->joinWith(['event', 'employee'])
                    ->andNotDeleted()
                    ->andConfirmed()
                    ->andWhere(['between', 'event.date', $start_date, $end_date])
                    ->andWhere(['employee.pp_mailing_list' => 1])
                    ->orderBy('employee.last_name')
                    ->all();

            $html = '';

            $html .= "<table>";
            $html .= "<tr><th>ID</th><th>Status</th><th>Last Name</th><th>First Name</th> </tr>";
            foreach ($employees as $key => $employeeShift) {
                if ($employeeShift->employee->status == 1) {
                    $status = "Active";
                } elseif ($employeeShift->employee->status == 2) {
                    $status = "Cadndidate";
                } elseif ($employeeShift->employee->status == 3) {
                    $status = "Hiatus";
                } elseif ($employeeShift->employee->status == 4) {
                    $status = "Inactive";
                } elseif ($employeeShift->employee->status == 5) {
                    $status = "Terminate";
                } elseif ($employeeShift->employee->status == 6) {
                    $status = "Resigned";
                } elseif ($employeeShift->employee->status == 10) {
                    $status = "Inactive 60";
                } elseif ($employeeShift->employee->status == 12) {
                    $status = "Inactive 180";
                } elseif ($employeeShift->employee->status == 14) {
                    $status = "Other";
                } else {
                    $status = '';
                }

                $html .= "<tr align='center'>
            	          <td>" . $employeeShift->employee->employee_id . "</td>
            	          <td>" . $status . "</td>
            	          <td with='20%'>" . $employeeShift->employee->last_name . "</td>
            	          <td with='20%'>" . $employeeShift->employee->first_name . "</td>
            	      </tr>";
            }
            $html .= "</table>";

            $pdf = new Pdf([
                // set to use core fonts only
                'mode' => Pdf::MODE_CORE,
                // A4 paper format
                'format' => Pdf::FORMAT_LETTER,
                // portrait orientation
                'orientation' => Pdf::ORIENT_PORTRAIT,
                'options' => ['title' => 'Paycheck Mailing List'],
                 // call mPDF methods on the fly
                'methods' => [
                    'SetHeader' => ['Paycheck Mailing List - Generated On: ' . date("r")],
                    'SetFooter' => ['{PAGENO}'],
                ]
            ]);

            $mpdf = $pdf->api;
            $mpdf->WriteHTML($html);

            $saveLocation = \Yii::getAlias('@webroot') . '/temp/' . $filename;
            $mpdf->Output($saveLocation, 'F');

            return $saveLocation;
        } elseif ($type == static::TYPE_GOLD_REPORT_MASTER) {
            $events = Event::find()
                        ->joinWith(['employees', 'client', 'venue'])
                        ->andWhere(['between', 'event.date', $start_date, $end_date])
                        ->andWhere(['confirmed' => 1, 'cancel_reason' => 0])
                        ->orderBy('client.name, venue.name, event.date')
                        ->all();

            $spreadsheet = new Spreadsheet();
            $sheet = $spreadsheet->getActiveSheet();
            $sheet->setShowGridlines(false);

            $sheet->getColumnDimension('A')->setAutoSize(true);
            $sheet->getColumnDimension('B')->setAutoSize(true);
            $sheet->getColumnDimension('C')->setAutoSize(true);
            $sheet->getColumnDimension('D')->setAutoSize(true);
            $sheet->getColumnDimension('E')->setAutoSize(true);
            $sheet->setCellValueByColumnAndRow(1, 1, 'Gold Report Sheet');
            $sheet->setCellValueByColumnAndRow(5, 1, date('m/d/Y', strtotime($end_date)));
            $sheet->setCellValueByColumnAndRow(1, 3, 'Day');
            $sheet->setCellValueByColumnAndRow(2, 3, 'Date');
            $sheet->setCellValueByColumnAndRow(3, 3, 'Client');
            $sheet->setCellValueByColumnAndRow(4, 3, 'Venue');
            $sheet->setCellValueByColumnAndRow(5, 3, 'Completed');

            $styleArray = [
                'font' => [
                    'bold' => true,
                ],
                'alignment' => [
                    'horizontal' => \PhpOffice\PhpSpreadsheet\Style\Alignment::HORIZONTAL_CENTER,
                ],
            ];

            $sheet->getStyle('A1')->applyFromArray($styleArray);
            $sheet->getStyle('D1')->applyFromArray($styleArray);
            $sheet->getStyle('A3')->applyFromArray($styleArray);
            $sheet->getStyle('B3')->applyFromArray($styleArray);
            $sheet->getStyle('C3')->applyFromArray($styleArray);
            $sheet->getStyle('D3')->applyFromArray($styleArray);
            $sheet->getStyle('E3')->applyFromArray($styleArray);

            $sheet->getStyle("A1:E1000")
                    ->getAlignment()
                    ->setHorizontal(\PhpOffice\PhpSpreadsheet\Style\Alignment::HORIZONTAL_CENTER);
            $index = 4;
            $venue_id = "";

            $border = [
                'borders' => [
                    'bottom' => [
                        'borderStyle' => \PhpOffice\PhpSpreadsheet\Style\Border::BORDER_THIN,
                    ],
                ],
            ];

            foreach ($events as $key => $event) {
                if ($venue_id !== $event->venue->venue_id) {
                    if ($index != 4) {
                        $sheet->getRowDimension($index)->setRowHeight(40);

                        $sheet->getStyle('A' . $index . ':F' . $index)->getFill()->setFillType(\PhpOffice\PhpSpreadsheet\Style\Fill::FILL_SOLID)->getStartColor()->setARGB('C0C0C0');

                        $index = $index + 1; // add blank row

                        $bold = [
                            'font' => [
                                'bold' => true,
                            ],
                        ];
                        $sheet->getStyle('F' . $index)->applyFromArray($bold);
                        $sheet->getStyle('E' . $index)->applyFromArray($border);
                        $sheet->setCellValueByColumnAndRow(6, $index, 'DONE');
                    } else {
                        // for fist line
                        $bold = [
                            'font' => [
                                'bold' => true,
                            ],
                        ];
                        $sheet->getStyle('F' . $index)->applyFromArray($bold);
                        $sheet->getStyle('E' . $index)->applyFromArray($border);
                        $sheet->setCellValueByColumnAndRow(6, $index, 'DONE');
                    }

                    $index = $index + 1;    // add seprate row
                }

                $venue_id = $event->venue->venue_id;

                $sheet->setCellValueByColumnAndRow(1, $index, date("l", strtotime($event->date)));
                $sheet->setCellValueByColumnAndRow(2, $index, date("m/d/Y", strtotime($event->date)));
                $sheet->setCellValueByColumnAndRow(3, $index, $event->client->name);
                $sheet->setCellValueByColumnAndRow(4, $index, $event->venue->name);

                $sheet->getStyle('E' . $index)->applyFromArray($border);

                $index++;
            }

            $filename = 'Gold Report Master-' . Date('YmdGis') . '.xlsx';
            $spreadsheet->getActiveSheet()->setTitle('Gold Report Master');

            $saveLocation = \Yii::getAlias('@webroot') . '/temp/' . $filename;
            $writer = new Xlsx($spreadsheet);
            $writer->save($saveLocation);

            $spreadsheet->disconnectWorksheets();
            unset($spreadsheet);

            return $saveLocation;
        } elseif ($type == static::TYPE_INDIVIDUAL_GOLD_SHEET) {
            $events = Event::find()
                        ->joinWith(['employees', 'client', 'venue'])
                        ->andWhere(['between', 'event.date', $start_date, $end_date])
                        ->andWhere(['confirmed' => 1, 'cancel_reason' => 0])
                        ->orderBy('client.name,venue.name,event.date')
                        ->all();

            $venue_ids = [];
            foreach ($events as $key => $event) {
                $venue_ids[] = $event->venue->venue_id;
            }
            $venue_ids = array_unique($venue_ids); // get venues

            $venues = Venue:: find()->select('name,venue_id')->where(['venue_id' => $venue_ids])->orderBy('name')->all();

            $spreadsheet = new Spreadsheet();
            $sheet = $spreadsheet->getActiveSheet();

            $sheet->setShowGridlines(false);
            $index = 0;
            $i = 0;
            foreach ($venues as $key => $venue) {
                foreach ($events as $key => $event) {
                    if ($venue->venue_id == $event->venue->venue_id) {
                        $venue_name = $venue->name;
                    }
                }

                $border = [
                    'borders' => [
                        'bottom' => [
                            'borderStyle' => \PhpOffice\PhpSpreadsheet\Style\Border::BORDER_THIN,
                        ],
                    ],
                ];

                $spreadsheet->setActiveSheetIndex($i)->getColumnDimension('A')->setAutoSize(true);
                $spreadsheet->setActiveSheetIndex($i)->getColumnDimension('B')->setAutoSize(true);
                $spreadsheet->setActiveSheetIndex($i)->getColumnDimension('C')->setAutoSize(true);
                $spreadsheet->setActiveSheetIndex($i)->getColumnDimension('D')->setAutoSize(true);
                $spreadsheet->setActiveSheetIndex($i)->getColumnDimension('E')->setAutoSize(true);
                $spreadsheet->setActiveSheetIndex($i)->setCellValueByColumnAndRow(1, 1, $venue_name);
                $spreadsheet->setActiveSheetIndex($i)->setCellValueByColumnAndRow(5, 1, date('m/d/Y', strtotime($end_date)));
                $spreadsheet->setActiveSheetIndex($i)->setCellValueByColumnAndRow(1, 3, 'Day');
                $spreadsheet->setActiveSheetIndex($i)->setCellValueByColumnAndRow(2, 3, 'Date');
                $spreadsheet->setActiveSheetIndex($i)->setCellValueByColumnAndRow(3, 3, 'Client');
                $spreadsheet->setActiveSheetIndex($i)->setCellValueByColumnAndRow(4, 3, 'Venue');
                $spreadsheet->setActiveSheetIndex($i)->setCellValueByColumnAndRow(5, 3, 'Completed');
                $styleArray = [
                    'font' => [
                        'bold' => true,
                    ],
                    'alignment' => [
                        'horizontal' => \PhpOffice\PhpSpreadsheet\Style\Alignment::HORIZONTAL_CENTER,
                    ],
                ];

                $spreadsheet->setActiveSheetIndex($i)->getStyle('A1:F4')->applyFromArray($styleArray);

                $spreadsheet->getActiveSheet()->getStyle("A1:F1000")
                        ->getAlignment()
                        ->setHorizontal(\PhpOffice\PhpSpreadsheet\Style\Alignment::HORIZONTAL_CENTER);

                $spreadsheet->setActiveSheetIndex($i)->setCellValueByColumnAndRow(6, 4, 'DONE');
                $spreadsheet->setActiveSheetIndex($i)->getStyle('E4')->applyFromArray($border);

                // Add some data
                $index = 5;
                foreach ($events as $key => $event) {
                    if ($venue->venue_id == $event->venue->venue_id) {
                        $spreadsheet->setActiveSheetIndex($i)->setCellValueByColumnAndRow(1, $index, date("l", strtotime($event->date)));
                        $spreadsheet->setActiveSheetIndex($i)->setCellValueByColumnAndRow(2, $index, date("m/d/Y", strtotime($event->date)));
                        $spreadsheet->setActiveSheetIndex($i)->setCellValueByColumnAndRow(3, $index, $event->client->name);
                        $spreadsheet->setActiveSheetIndex($i)->setCellValueByColumnAndRow(4, $index, $event->venue->name);
                        $spreadsheet->setActiveSheetIndex($i)->getStyle('E' . $index)->applyFromArray($border);
                        $index++;
                    }
                }
                $spreadsheet->createSheet();

                $venue_name = substr($venue_name, 0, 29);
                $venue_name = str_replace('&', 'and', $venue_name);
                $venue_name = str_replace('/', ',', $venue_name);

                $spreadsheet->getActiveSheet()->setTitle('A' . $i);

                $i++;
            }

            $spreadsheet->setActiveSheetIndex(0);

            $spreadsheet->removeSheetByIndex($i); // remove last default sheet

            $filename = 'Individual Gold Sheet-' . Date('YmdGis') . '.xlsx';

            $saveLocation = \Yii::getAlias('@webroot') . '/temp/' . $filename;
            $writer = new Xlsx($spreadsheet);
            $writer->save($saveLocation);

            $spreadsheet->disconnectWorksheets();
            unset($spreadsheet);

            return $saveLocation;
        } elseif ($type == static::TYPE_DOUBLE_SHIFT) {
            $dup_shifts
                = (new Query())
                    ->select([
                        'shift_employee.shift_employee_id',
                        'shift_employee.employee_id',
                        'event.date as shift_date'
                    ])
                    ->from('shift_employee')
                    ->innerJoin('event', 'event.event_id = shift_employee.event_id')
                    ->andWhere('shift_employee.deleted_at IS NULL')
                    ->andWhere(['confirmed' => 1, 'cancel_reason' => 0])
                    ->andWhere(['between', 'event.date', $start_date, $end_date])
                    ->groupBy('event.date, shift_employee.employee_id')
                    ->having('COUNT(DISTINCT shift_employee.shift_employee_id) > 1')
                    ->all();

            $dates = [];
            $employee_id = [];
            foreach ($dup_shifts as $shift) {
                $dates[] = $shift['shift_date'];
                $employee_id[] = $shift['employee_id'];
            }



            $employees
                = ShiftEmployee::find()
                    ->select([
                        'shift_employee.employee_id',
                        'shift_employee.event_id',
                        'shift_employee.shift_employee_id',
                        'approved_by'
                    ])
                    ->joinWith([
                        'event',
                        'employee'
                    ])
                    ->with([
                        'timesheet',
                        'event.venue',
                        'approvedBy',
                        'event.client'
                    ])
                    ->andNotDeleted()
                    ->andConfirmed()
                    ->andWhere([
                        'event.date' => $dates,
                        'shift_employee.employee_id' => $employee_id
                    ])
                    ->andWhere([
                        'NOT IN',
                        'employee.status',
                        [Employee::STATUS_CANDIDATE]
                    ])
                    ->orderBy('event.date desc, shift_employee.employee_id')
                    ->all();

            $spreadsheet = new Spreadsheet();
            $sheet = $spreadsheet->getActiveSheet();

            $sheet->setCellValue('A1', 'Employee ID');
            $sheet->setCellValue('B1', 'Name');
            $sheet->setCellValue('C1', 'Date');
            $sheet->setCellValue('D1', 'Venue');
            $sheet->setCellValue('E1', 'Hours');
            $sheet->setCellValue('F1', 'Approved By');

            $bold = [
                'font' => [
                    'bold' => true,
                ],
            ];
            $sheet->getStyle('A1:F1')->applyFromArray($bold);

            foreach (range('A', 'F') as $columnID) {
                $sheet->getColumnDimension($columnID)->setAutoSize(true);
            }
            $index = 2;

            foreach ($employees as $key => $employee) {
                foreach ($dup_shifts as $shift) {
                    if (strtotime($shift['shift_date']) == strtotime($employee->event->date) && ($shift['employee_id'] == $employee->employee_id)) {
                        $sheet->setCellValue('A' . $index, $employee->employee_id);
                        $sheet->setCellValue('B' . $index, $employee->employee->first_name . ' ' . $employee->employee->last_name);
                        $sheet->setCellValue('C' . $index, date("d/m/Y", strtotime($employee->event->date)));
                        $sheet->setCellValue('D' . $index, $employee->event->venue->name);

                        // get payroll hours
                        $seconds = 0;
                        if (!empty($employee->timesheet->timesheet_id)) {
                            $seconds = Utils::payroll_hours($employee->timesheet, $employee->event->client);
                        }

                        $hours = ($seconds / 3600);

                        $sheet->setCellValue('E' . $index, $hours);
                        $sheet->setCellValue('F' . $index, (!empty($employee->approved_by) ? $employee->approvedBy->first_name . ' ' . $employee->approvedBy->last_name : ''));
                        $index++;
                    }
                }
            }


            $filename = 'Double shift-' . Date('YmdGis') . '.xlsx';

            $saveLocation = \Yii::getAlias('@webroot') . '/temp/' . $filename;
            $writer = new Xlsx($spreadsheet);
            $writer->save($saveLocation);

            $spreadsheet->disconnectWorksheets();
            unset($spreadsheet);

            return $saveLocation;
        } elseif ($type == 13) {
            // 7 Days Worked

            $period
                = new \DatePeriod(
                    new \DateTime($start_date), new \DateInterval('P1D'), new \DateTime($end_date . " +1 day")
                );

            $dates = [];
            foreach ($period as $key => $value) {
                $dates[] = $value->format('Y-m-d');
            }

            $employees
                = Employee::find()
                    ->joinWith('shifts')
                    ->with([
                        'shifts.event.venue',
                        'shifts.timesheet',
                        'shifts.event.client',
                        'shifts.approvedBy'
                    ])
                    ->where(['between', 'event.date', $start_date, $end_date])
                    ->andWhere(['event.date' => $dates])
                    ->andWhere(['confirmed' => 1, 'cancel_reason' => 0])
                    ->andWhereNotCandidate()
                    ->groupBy('employee.employee_id')
                    ->having('COUNT(DISTINCT event.date) =' . count($dates))
                    ->all();

            $spreadsheet = new Spreadsheet();
            $sheet = $spreadsheet->getActiveSheet();

            $sheet->setCellValue('A1', 'Employee ID');
            $sheet->setCellValue('B1', 'Name');
            $sheet->setCellValue('C1', 'Date');
            $sheet->setCellValue('D1', 'Venue');
            $sheet->setCellValue('E1', 'Timesheet Hours');
            $sheet->setCellValue('F1', 'Scheduled Hours');
            $sheet->setCellValue('G1', 'Confirmed At');
            $sheet->setCellValue('H1', 'Approved By');
            $sheet->setCellValue('I1', 'Who Pays');

            $bold = [
                'font' => [
                    'bold' => true,
                ],
            ];
            $sheet->getStyle('A1:F1')->applyFromArray($bold);
            $sheet->getStyle('A2')->applyFromArray($bold);

            foreach (range('A', 'F') as $columnID) {
                $sheet->getColumnDimension($columnID)->setAutoSize(true);
            }

            $sheet->setCellValue('A2', date("d/m/Y", strtotime($start_date)) . " - " . date("d/m/Y", strtotime($end_date)));

            $index = 3;
            foreach ($employees as $employee) {
                /* @var $employee Employee */

                $shiftEmployeeQuery = $employee->getShiftEmployees();
                /* @var $shiftEmployeeQuery */
                $shiftEmployeeQuery->andConfirmed();
                $shiftEmployeeQuery->andInDateRange($start_date, $end_date);
                $shiftEmployeeQuery->orderBy('confirmed_at ASC');

                foreach ($shiftEmployeeQuery->each() as $shiftEmployee) {
                    /* @var $shiftEmployee ShiftEmployee */

                    $sheet->setCellValue('A' . $index, $employee->employee_id);
                    $sheet->setCellValue('B' . $index, $employee->first_name . ' ' . $employee->last_name);
                    $sheet->setCellValue('C' . $index, date("m/d/Y", strtotime($shiftEmployee->event->date)));
                    $sheet->setCellValue('D' . $index, $shiftEmployee->event->venue->name);

                    // get payroll hours
                    $seconds = Utils::payroll_hours($shiftEmployee->timesheet, $shiftEmployee->event->client);
                    $hours = ($seconds / 3600);

                    $sheet->setCellValue('E' . $index, $hours);

                    $shift = $shiftEmployee->shiftPosition->shift;
                    /* @var $shift \common\models\Shift */

                    $scheduledHours = $shift->getWorkHours();

                    $sheet->setCellValue('F' . $index, $scheduledHours);

                    $sheet->setCellValue(
                        'G' . $index,
                        (
                            $shiftEmployee->confirmed_at
                                ? date('m/d/Y H:i:s', strtotime($shiftEmployee->confirmed_at))
                                : ''
                        )
                    );

                    $sheet->setCellValue(
                        'H' . $index,
                        (
                            $shiftEmployee->approvedBy
                                ? \common\models\User::getFullnameBy($shiftEmployee->approvedBy)
                                : ''
                        )
                    );

                    if ($end_date == $shiftEmployee->event->date) {
                        $overtimePaidBy = $shiftEmployee->getOvertimePaidByLabel();
                    } else {
                        $overtimePaidBy = 'N/A';
                    }

                    $sheet->setCellValue('I' . $index, $overtimePaidBy);

                    $index++;
                }
            }

            $filename = '7 Day shift-' . Date('YmdGis') . '.xlsx';

            $saveLocation = \Yii::getAlias('@webroot') . '/temp/' . $filename;
            $writer = new Xlsx($spreadsheet);
            $writer->save($saveLocation);

            $spreadsheet->disconnectWorksheets();
            unset($spreadsheet);

            return $saveLocation;
        } elseif (in_array($type, [static::TYPE_OVER_40_HOURS, static::TYPE_OVER_40_HOURS_SCHEDULE_HOURS])) {
            return Job::enqueue($getJobName($type), [
                \common\jobs\Over40HoursExportJob::class,
                [
                    'type' => $type,
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                ]
            ]);
        } elseif (in_array($type, [static::TYPE_OVERTIMES])) {
            return Job::enqueue($getJobName($type), [
                \common\jobs\OvertimeExportJob::class,
                [
                    'type' => $type,
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                ]
            ]);
        } elseif ($type == 15) {
            $scikemps = SickWageEmployee::find()->joinWith(['sickWagePay', 'sickWagePay.workerComp'])->with(['sickWageEmployee'])->where(['sick_wage_pay.type' => 1, 'sick_wage_pay.date_range' => $dateRange])->orderBy('sick_wage_employee.sick_wage_employee_id desc')->all();

            $spreadsheet = new Spreadsheet();
            $sheet = $spreadsheet->getActiveSheet();

            $sheet->setCellValue('A1', 'Employee ID');
            $sheet->setCellValue('B1', 'Employee Name');
            $sheet->setCellValue('C1', 'Hours');
            $sheet->setCellValue('D1', 'Worker’s Comp Code');
            $sheet->setCellValue('E1', 'Pay Rate');
            $sheet->setCellValue('F1', 'Gross Pay');


            $sheet->getStyle('E')->getNumberFormat()->setFormatCode(\PhpOffice\PhpSpreadsheet\Style\NumberFormat::FORMAT_CURRENCY_USD_SIMPLE);
            $sheet->getStyle('F')->getNumberFormat()->setFormatCode(\PhpOffice\PhpSpreadsheet\Style\NumberFormat::FORMAT_CURRENCY_USD_SIMPLE);

            $bold = [
                'font' => [
                    'bold' => true,
                ],
            ];
            $sheet->getStyle('A1:F1')->applyFromArray($bold);
            $sheet->getStyle('A2')->applyFromArray($bold);

            foreach (range('A', 'F') as $columnID) {
                $sheet->getColumnDimension($columnID)->setAutoSize(true);
            }
            $sheet->setCellValue('A2', date("d/m/Y", strtotime($start_date)) . " - " . date("d/m/Y", strtotime($end_date)));
            $index = 3;
            foreach ($scikemps as $key => $employee) {
                $sheet->setCellValue('A' . $index, $employee->sickWageEmployee->employee_id);
                $sheet->setCellValue('B' . $index, $employee->sickWageEmployee->first_name . ' ' . $employee->sickWageEmployee->last_name);
                $sheet->setCellValue('C' . $index, $employee->sickWagePay->hours);
                $sheet->setCellValue('D' . $index, $employee->sickWagePay->workerComp->wc_code);
                $sheet->setCellValue('E' . $index, $employee->sickWagePay->pay_rate);
                $sheet->setCellValue('F' . $index, $employee->sickWagePay->hours * $employee->sickWagePay->pay_rate);

                $index++;
            }



            $filename = 'Sick Pay Report -' . Date('YmdGis') . '.xlsx';

            $saveLocation = \Yii::getAlias('@webroot') . '/temp/' . $filename;
            $writer = new Xlsx($spreadsheet);
            $writer->save($saveLocation);

            $spreadsheet->disconnectWorksheets();
            unset($spreadsheet);

            return $saveLocation;
        } elseif ($type == 16) {
            $scikemps = SickWageEmployee::find()->joinWith(['sickWagePay', 'sickWagePay.workerComp'])->with(['sickWageEmployee'])->where(['sick_wage_pay.type' => 2, 'sick_wage_pay.date_range' => $dateRange])->orderBy('sick_wage_employee.sick_wage_employee_id desc')->all();

            $spreadsheet = new Spreadsheet();
            $sheet = $spreadsheet->getActiveSheet();

            $sheet->setCellValue('A1', 'Employee ID');
            $sheet->setCellValue('B1', 'Employee Name');
            $sheet->setCellValue('C1', 'Hours');
            $sheet->setCellValue('D1', 'Worker’s Comp Code');
            $sheet->setCellValue('E1', 'Pay Rate');
            $sheet->setCellValue('F1', 'Gross Pay');

            $sheet->getStyle('E')->getNumberFormat()->setFormatCode(\PhpOffice\PhpSpreadsheet\Style\NumberFormat::FORMAT_CURRENCY_USD_SIMPLE);
            $sheet->getStyle('F')->getNumberFormat()->setFormatCode(\PhpOffice\PhpSpreadsheet\Style\NumberFormat::FORMAT_CURRENCY_USD_SIMPLE);

            $bold = [
                'font' => [
                    'bold' => true,
                ],
            ];
            $sheet->getStyle('A1:F1')->applyFromArray($bold);
            $sheet->getStyle('A2')->applyFromArray($bold);

            foreach (range('A', 'F') as $columnID) {
                $sheet->getColumnDimension($columnID)->setAutoSize(true);
            }
            $sheet->setCellValue('A2', date("d/m/Y", strtotime($start_date)) . " - " . date("d/m/Y", strtotime($end_date)));
            $index = 3;
            foreach ($scikemps as $key => $employee) {
                $sheet->setCellValue('A' . $index, $employee->sickWageEmployee->employee_id);
                $sheet->setCellValue('B' . $index, $employee->sickWageEmployee->first_name . ' ' . $employee->sickWageEmployee->last_name);
                $sheet->setCellValue('C' . $index, $employee->sickWagePay->hours);
                $sheet->setCellValue('D' . $index, $employee->sickWagePay->workerComp->wc_code);
                $sheet->setCellValue('E' . $index, $employee->sickWagePay->pay_rate);
                $sheet->setCellValue('F' . $index, $employee->sickWagePay->hours * $employee->sickWagePay->pay_rate);

                $index++;
            }


            $filename = 'Wage Replacement Report -' . Date('YmdGis') . '.xlsx';

            $saveLocation = \Yii::getAlias('@webroot') . '/temp/' . $filename;
            $writer = new Xlsx($spreadsheet);
            $writer->save($saveLocation);

            $spreadsheet->disconnectWorksheets();
            unset($spreadsheet);

            return $saveLocation;
        } elseif (in_array($type, [static::TYPE_FACTORED_INVOICES, static::TYPE_NON_FACTORED_INVOICES])) {
            // 17 => 'Factored Invoices', 18 => 'Non-Factored Invoices', 20 => 'Unfactored Invoices',
            return Job::enqueue($getJobName($type), [
                jobs\FactoredInvoicesExportJob::class,
                [
                    'type' => $type,
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                ]
            ]);
        } elseif (in_array($type, [static::TYPE_INVOICES])) {
            return Job::enqueue($getJobName($type), [
                jobs\InvoicesExportJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                    'invoiceStartNo' => $this->invoie_start_no,
                ]
            ]);
        } elseif ($type == static::TYPE_DISCREPANCY) {
            // Discrepancy Report

            $headers = [
                'Event Date',
                'Venue',
                'Employee ID',
                'First Name',
                'Last Name',
                'Employee Hours',
                'Client Hours',
                'Employee Worked',
                'Client Worked',
                'Discrepancy Hours',
                'Timesheet Missing',
                'Timesheet Received',
                'Employee Date',
                'Employee Time',
                'Employee Phone',
                'Staffing Manager',
                'Employee Notes',
                'Client Notes',
                'Payroll/Report Notes',
            ];

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

            $bold = [
                'font' => [
                    'bold' => true,
                ],
            ];
            $sheet->getStyle(join(':', ['A1', "{$lastColumnName}1"]))->applyFromArray($bold);

            foreach (range(1, $lastColumnIndex) as $columnIndex) {
                $columnID = Coordinate::stringFromColumnIndex($columnIndex);

                $sheet->getColumnDimension($columnID)->setAutoSize(true);
            }

            foreach ($headers as $header) {
                $columnName = $columnNamesByHeader[$header];
                $sheet->setCellValue("{$columnName}1", $header);
            }

            $timesheets
                = Timesheet::find()
                    ->joinWith([
                        'event',
                        'employeeShift' => function (\common\models\query\ShiftEmployeeQuery $query) {
                            $query->andValidForPayroll();
                        },
                    ])
                    ->with(['employeeShift.shiftPosition.position', 'event.venue', 'employee'])
                    ->andWhere("use_sheet IS NULL")
                    ->andWhere([
                        'IN',
                        'timesheet.shift_employee_id',
                        (function () use (&$start_date, &$end_date) {
                            return
                                ShiftEmployee::find()
                                    ->joinWith('event')
                                    ->andWhere(['BETWEEN', 'event.date', $start_date, $end_date])
                                    ->andValidForPayroll()
                                    ->select('shift_employee_id');
                        })()
                    ])
                    ->orderBy('event.client_id, event.venue_id, event.date, event.event_id')
                    ->all();

            $i = 2;
            foreach ($timesheets as $t) {
                /* @var $t Timesheet */

                $notesQuery
                    = (new Query())
                        ->select('note')
                        ->from('employee_note')
                        ->andWhere(['shift_employee_id' => $t->shift_employee_id])
                        ->andWhere(['LIKE', 'type', EmployeeNote::NOTE_TYPE_PAYROLL]);

                $notes = strip_tags(join(' | ', $notesQuery->column()));

                $timesheetAnalyzer = $t->analyze();
                /* @var $timesheetAnalyzer \common\components\payroll\BaseTimesheetAnalyzer */

                $clientSeconds = (int) $timesheetAnalyzer->getSecondsByClient(false);
                $employeeSeconds = (int) $timesheetAnalyzer->getSecondsByEmployee(false);

                $clientHours = floatval($clientSeconds / 3600);
                $employeeHours = floatval($employeeSeconds / 3600);

                $discrepancy = $timesheetAnalyzer->getDiscrepancy();

                if (!$discrepancy && $clientHours && $employeeHours) {
                    continue;
                }

                if (
                    $clientSeconds == 0
                    && $employeeSeconds == 0
                ) {
                    $sheet->setCellValue($columnNamesByHeader['Event Date'] . $i, date('m/d/Y', strtotime($t->event->date)));
                    $sheet->setCellValue($columnNamesByHeader['Venue'] . $i, $t->event->venue->name);
                    $sheet->setCellValue($columnNamesByHeader['Employee ID'] . $i, $t->employee->payroll_id);
                    $sheet->setCellValue($columnNamesByHeader['First Name'] . $i, $t->employee->first_name);
                    $sheet->setCellValue($columnNamesByHeader['Last Name'] . $i, $t->employee->last_name);
                    $sheet->setCellValue($columnNamesByHeader['Employee Hours'] . $i, 'No Hours');
                    $sheet->setCellValue($columnNamesByHeader['Client Hours'] . $i, 'No Hours');
                    $sheet->setCellValue(
                        $columnNamesByHeader['Employee Worked'] . $i,
                        ArrayHelper::getValue(Timesheet::getWorkedLabels(), $t->employee_worked)
                    );
                    $sheet->setCellValue(
                        $columnNamesByHeader['Client Worked'] . $i,
                        ArrayHelper::getValue(Timesheet::getWorkedLabels(), $t->client_worked)
                    );
                    $sheet->setCellValue($columnNamesByHeader['Discrepancy Hours'] . $i, "");
                    $sheet->setCellValue($columnNamesByHeader['Timesheet Missing'] . $i, "Both");
                    $sheet->setCellValue($columnNamesByHeader['Timesheet Received'] . $i, ($t->event->timesheet_received ? 'yes' : ''));
                    $sheet->setCellValue($columnNamesByHeader['Employee Date'] . $i, "");
                    $sheet->setCellValue($columnNamesByHeader['Employee Time'] . $i, "");
                    $sheet->setCellValue($columnNamesByHeader['Employee Phone'] . $i, $t->employee->mobile);
                    $sheet->setCellValue($columnNamesByHeader['Staffing Manager'] . $i, (
                    $t->event->venue->staffingmanager
                        ? \common\models\User::getFullnameBy($t->event->venue->staffingmanager)
                        : ''
                    ));
                    $sheet->setCellValue($columnNamesByHeader['Employee Notes'] . $i, $t->employee_notes);
                    $sheet->setCellValue($columnNamesByHeader['Client Notes'] . $i, $t->client_notes);
                    $sheet->setCellValue($columnNamesByHeader['Payroll/Report Notes'] . $i, $notes);

                    $i++;
                } else {
                    $discrepancyHours = abs($clientHours - $employeeHours);

                    $missing = '';
                    if (
                            ($clientHours == 0)
                            && $employeeHours == 0
                            ) {
                        $missing = "Both";
                    } elseif ($clientHours == 0) {
                        $missing = "Client";
                    } elseif ($employeeHours == 0) {
                        $missing = "Employee";
                    }

                    $submit_date = $t->employee_submit_date ? date('m/d/Y', strtotime($t->employee_submit_date)) : '';
                    $submit_time = $t->employee_submit_date ? date('h:i a', strtotime($t->employee_submit_date)) : '';

                    $sheet->setCellValue($columnNamesByHeader['Event Date'] . $i, date('m/d/Y', strtotime($t->event->date)));
                    $sheet->setCellValue($columnNamesByHeader['Venue'] . $i, $t->event->venue->name);
                    $sheet->setCellValue($columnNamesByHeader['Employee ID'] . $i, $t->employee->payroll_id);
                    $sheet->setCellValue($columnNamesByHeader['First Name'] . $i, $t->employee->first_name);
                    $sheet->setCellValue($columnNamesByHeader['Last Name'] . $i, $t->employee->last_name);
                    $sheet->setCellValue(
                        $columnNamesByHeader['Employee Hours'] . $i,
                        (
                            is_float($employeeHours)
                                ? $formatter->asDecimal($employeeHours, 2)
                                : $employeeHours
                        )
                    );
                    $sheet->setCellValue(
                        $columnNamesByHeader['Client Hours'] . $i,
                        (
                            is_float($clientHours)
                                ? $formatter->asDecimal($clientHours, 2)
                                : $clientHours
                        )
                    );
                    $sheet->setCellValue(
                        $columnNamesByHeader['Employee Worked'] . $i,
                        ArrayHelper::getValue(Timesheet::getWorkedLabels(), $t->employee_worked)
                    );
                    $sheet->setCellValue(
                        $columnNamesByHeader['Client Worked'] . $i,
                        ArrayHelper::getValue(Timesheet::getWorkedLabels(), $t->client_worked)
                    );
                    $sheet->setCellValue($columnNamesByHeader['Discrepancy Hours'] . $i, $discrepancyHours);
                    $sheet->setCellValue(
                        $columnNamesByHeader['Timesheet Missing'] . $i,
                        $missing
                    );
                    $sheet->setCellValue($columnNamesByHeader['Timesheet Received'] . $i, ($t->event->timesheet_received ? 'yes' : ''));
                    $sheet->setCellValue($columnNamesByHeader['Employee Date'] . $i, $submit_date);
                    $sheet->setCellValue($columnNamesByHeader['Employee Time'] . $i, $submit_time);
                    $sheet->setCellValue($columnNamesByHeader['Employee Phone'] . $i, $t->employee->mobile);
                    $sheet->setCellValue(
                        $columnNamesByHeader['Staffing Manager'] . $i,
                        (
                            $t->event->venue->staffingmanager
                                ? \common\models\User::getFullnameBy($t->event->venue->staffingmanager)
                                : ''
                        )
                    );
                    $sheet->setCellValue($columnNamesByHeader['Employee Notes'] . $i, $t->employee_notes);
                    $sheet->setCellValue($columnNamesByHeader['Client Notes'] . $i, $t->client_notes);
                    $sheet->setCellValue($columnNamesByHeader['Payroll/Report Notes'] . $i, $notes);

                    $i++;
                }
            }

            $filename = 'discrepancy-' . Date('YmdGis') . '.xlsx';

            $saveLocation = \Yii::getAlias('@webroot') . '/temp/' . $filename;
            $writer = new Xlsx($spreadsheet);
            $writer->save($saveLocation);

            $spreadsheet->disconnectWorksheets();
            unset($spreadsheet);

            return $saveLocation;
        } elseif ($type == static::TYPE_1ST_SHIFT_EMPLOYEES) {
            // 1st Shift - Employees

            $sql = "SELECT 
                        em.payroll_id, 
                        em.first_name, 
                        em.last_name,
                        em.start_date,
                        em.start_date2,
                        em.concierge_date,
                        (
                            SELECT 
                                MIN(`event`.`date`)
                             FROM 
                                `event` 
                                INNER JOIN shift_employee se 
                                    ON (`event`.`event_id` = se.event_id) 
                             WHERE 
                                `event`.date >= :from  
                                AND se.deleted_at IS NULL 
                                AND `event`.date <= :to  
                                AND se.confirmed = 1 
                                AND se.cancel_reason = 0 
                                AND se.employee_id = em.employee_id
                                AND (
                                    em.start_date2 IS NULL
                                    OR IF(
                                        em.start_date2 >= :from
                                        AND em.start_date2 <= :to
                                        AND EXISTS(
                                            SELECT
                                                *
                                            FROM
                                                shift_employee se
                                                INNER JOIN `event` e
                                                    ON (se.event_id = e.event_id)
                                            WHERE
                                                se.deleted_at IS NULL
                                                AND se.employee_id = em.employee_id
                                                AND se.confirmed = 1
                                                AND se.cancel_reason = 0
                                                AND e.date < :from
                                        ),
                                        event.date > em.start_date2,
                                        1
                                    )
                                )
                        ) as `first_date`, 
                        em.email,
                        em.mobile, 
                        em.status, 
                        refe.first_name as refe_first_name,
                        refe.last_name as refe_last_name,
                        refe.payroll_id as refe_payroll_id,
                        recu.first_name as recu_first_name,
                        recu.last_name as recu_last_name, 
                        c.name as county_name,
                        emwr.description as work_state
                    FROM 
                        employee em 
                        LEFT JOIN employee refe 
                            ON (em.referred_by = refe.employee_id) 
                        LEFT JOIN user recu 
                            ON (em.recruited_by = recu.id) 
                        LEFT JOIN county c 
                            ON (em.county_id = c.id) 
                        LEFT JOIN min_wage_rate emwr
                            ON (em.min_wage_id = emwr.min_wage_id)
                    WHERE 
                        em.employee_id IN (
                            SELECT 
                                se.employee_id 
                            FROM 
                                shift_employee se
                                INNER JOIN `event` e 
                                    ON (se.event_id = e.event_id) 
                                INNER JOIN timesheet on se.shift_employee_id = timesheet.shift_employee_id
                            WHERE 
                                se.deleted_at IS NULL 
                                AND se.confirmed = 1 
                                AND se.cancel_reason = 0
                                AND e.date <= :to 
                                AND timesheet.employee_worked IN ('" . Timesheet::WORKED_WORKED . "', '" . Timesheet::WORKED_SENTHOME . "')
                                AND timesheet.client_worked IN ('" . Timesheet::WORKED_WORKED . "', '" . Timesheet::WORKED_SENTHOME . "')
                        )
                        AND (
                            em.employee_id NOT IN (
                                SELECT 
                                    se.employee_id 
                                FROM 
                                    shift_employee se
                                    INNER JOIN `event` e 
                                        ON (se.event_id = e.event_id) 
                                WHERE 
                                    se.deleted_at IS NULL 
                                    AND se.confirmed = 1 
                                    AND se.cancel_reason = 0 
                                    AND e.date < :from
                            )
                            OR em.employee_id IN (
                                SELECT
                                    se.employee_id
                                FROM
                                    shift_employee se
                                    INNER JOIN `event` e
                                        ON (se.event_id = e.event_id)
                                WHERE
                                    se.deleted_at IS NULL
                                    AND em.start_date2 IS NOT NULL
                                    AND se.confirmed = 1
                                    AND se.cancel_reason = 0
                                    AND e.date < :to
                                    AND e.date > :from
                                    AND e.date >= em.start_date2
                            )
                        );";

            $command
                = $db->createCommand($sql, [
                    ':from' => $this->date_from,
                    ':to' => $this->date_to
                ]);

            $buffer = fopen('php://temp', 'r+');
            fputcsv($buffer, [
                'Payroll ID',
                'First Name',
                'Last Name',
                'Status',
                'County of Residence',
                'Hire Date',
                'Rehire Date',
                '1st Date Worked',
                'Email Address',
                'Phone',
                'Referred by',
                'Recruited by',
                'Work State',
                'Concierge Date',
            ]);

            foreach ($command->query() as $row) {
                fputcsv($buffer, [
                    $row['payroll_id'],
                    $row['first_name'],
                    $row['last_name'],
                    ArrayHelper::getValue(Employee::getStatusLabels(), $row['status']),
                    $row['county_name'],
                    date('m/d/Y', strtotime($row['start_date'])),
                    (empty($row['start_date2']) ? '' : date('m/d/Y', strtotime($row['start_date2']))),
                    date('m/d/Y', strtotime($row['first_date'])),
                    $row['email'],
                    $row['mobile'],
                    (
                        $row['refe_first_name'] || $row['refe_last_name']
                            ? Employee::getFullnameBy($row, 'refe', false)
                                . ($row['refe_payroll_id'] ? " ({$row['refe_payroll_id']})" : '')
                            : ''
                    ),
                    (
                        $row['recu_first_name'] || $row['recu_last_name']
                            ? User::getFullnameBy($row, 'recu', false)
                            : ''
                    ),
                    $row['work_state'],
                    $row['concierge_date'],
                ]);
            }

            rewind($buffer);
            $contents = stream_get_contents($buffer);

            fclose($buffer);

            return [
                'contents' => $contents,
                'fileName'
                    => join('-', [
                        '1st-shifts',
                        date('mdY', strtotime($this->date_from)),
                        date('mdY', strtotime($this->date_to)),
                    ]) . '.csv',
            ];
        } elseif ($type == static::TYPE_OPEN_SHIFTS) {
            return Job::enqueue($getJobName($type), [
                \common\jobs\OpenShiftsExportJob::class,
                [
                    'startDate' => $this->date_from,
                    'endDate' => $this->date_to,
                ]
            ]);
        } elseif ($type == 27) {
            return Job::enqueue('New Positions', [
                \common\jobs\NewPositionsExportJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                ]
            ]);
        } elseif ($type == 28) {
            return Job::enqueue('Candidate Partners', [
                \common\jobs\CandidatePartnersExportJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                ]
            ]);
        } elseif ($type == static::TYPE_EMPLOYEE_TENURE_AND_SHIFT_COUNT) {
            return Job::enqueue('Employee Tenure and Shift Count', [
                \common\jobs\EmployeeTenureExportJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                ]
            ]);
        } elseif ($type == static::TYPE_EMPLOYEE_DOCUMENTS) {
            return Job::enqueue('Concierge Temperature Check', [
                \common\jobs\ConciergeTemperatureCheckExportJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                    'documentTypeId' => $this->employee_document_type_id,
                ]
            ]);
        } elseif ($type == static::TYPE_CLIENT_AND_VENUE_DOCUMENTS) {
            return Job::enqueue('Employee Documents', [
                \common\jobs\ClientVenueDocumentExportJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                    'documentTypeId' => $this->client_document_type_id,
                ]
            ]);
        } elseif ($type == static::TYPE_SHIFT_NOSHOW) {
            return Job::enqueue('Shift No Show', [
                \common\jobs\ShiftNoShowExportJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                ]
            ]);
        } elseif (in_array($type, [
            static::TYPE_SHIFT_CANCELLED_LESS_24,
            static::TYPE_SHIFT_CANCELLED_NO_CALL_NO_SHOW,
        ])) {
            $name
                = static::TYPE_SHIFT_CANCELLED_NO_CALL_NO_SHOW == $type
                    ? 'Shift Cancelled No Call No Show'
                    : 'Shift Cancelled Less 24';

            return Job::enqueue($name, [
                \common\jobs\ShiftCancelledExportJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                    'type' => $type,
                    'cancelReason'
                        => (
                            static::TYPE_SHIFT_CANCELLED_NO_CALL_NO_SHOW == $type
                                ? \common\models\ShiftEmployee::CANCEL_REASON_NO_CALL_NO_SHOW
                                : \common\models\ShiftEmployee::CANCEL_REASON_EMPLOYEE_CANCELLED_LESS_24
                        )
                ]
            ]);
        } elseif ($type == static::TYPE_SHIFT_LATE_ARRIVALS) {
            return Job::enqueue('Shift Late Arrivals', [
                \common\jobs\ShiftLateArrivals::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                ]
            ]);
        } elseif ($type == static::TYPE_CERTIFICATIONS) {
            return Job::enqueue('Certifications', [
                \common\jobs\CertificationExportJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                ]
            ]);
        } elseif ($type == static::TYPE_EMPLOYEE_SHIFT_CANCELlATIONS) {
            return Job::enqueue('Employee Shift Cancellations', [
                \common\jobs\EmployeeShiftCancellationExportJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                    'venueId' => $this->venue_id,
                    'employeeId' => $this->employee_id,
                    'employeeCancelReason' => $this->employee_cancel_reason,
                ]
            ]);
        } elseif ($type == static::TYPE_EMPLOYEE_LIST) {
            return Job::enqueue($getJobName($type), [
                \common\jobs\EmployeeListReportJob::class,
                [
                    'userId' => $this->_actor ? $this->_actor->id : null,
                ]
            ]);
        } elseif ($type == static::TYPE_BENEFIT_SICK) {
            return Job::enqueue($getJobName($type), [
                BenefitSickExportJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date
                ]
            ]);
        } elseif ($type == static::TYPE_TIMESHEET_EMPLOYEE_SUBMITTED_HOURS) {
            return Job::enqueue($getJobName($type), [
                \common\jobs\TimesheetEmployeeSubmittedHoursJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                    'venueId' => $this->venue_id,
                    'employeeId' => $this->employee_id,
                ]
            ]);
        } elseif ($type == static::TYPE_TIMESHEET_CLIENT_SUBMITTED_HOURS) {
            return Job::enqueue($getJobName($type), [
                \common\jobs\TimesheetClientSubmittedHoursJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                    'venueId' => $this->venue_id,
                    'employeeId' => $this->employee_id,
                ]
            ]);
        } elseif ($type == static::TYPE_MSP_MARRIOTT) {
            return Job::enqueue($getJobName($type), [
                \common\jobs\MspMarriottJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                ]
            ]);
        } elseif ($type == static::TYPE_NEW_HIRES) {
            return Job::enqueue($getJobName($type), [
                \common\jobs\NewHiresExportJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                ]
            ]);
        } elseif ($type == static::TYPE_CLIENT_BILLING) {
            return \common\models\Job::enqueue($getJobName($type), [
                \common\jobs\ClientBillingExportJob::class,
                []
            ]);
        } elseif ($type == static::TYPE_HUMAN_RESOURCES) {
            return \common\models\Job::enqueue($getJobName($type), [
                \common\jobs\HumanResourcesExportJob::class,
                [
                    'startDate' => $start_date,
                    'endDate' => $end_date,
                ]
            ]);
        } else {
            die("Please choose types");
        }
    }

    public function number_format($number = '')
    {
        return Yii::$app->formatter->asDecimal($number, 2); // number_format((float) $number, 2, '.', ',');
    }

    const TYPE_CLIENTS = 1;
    const TYPE_EMPLOYEE = 10;
    const TYPE_ALL_CLIENTS_EMPLOYEES = 19;
    const TYPE_ALL_CLIENTS_EMPLOYEES_NEW = 192;
    const TYPE_TIMESHEET_VERIFICATION = 43;
    const TYPE_TIMESHEET_VERIFICATION_CLIENTS = 44;
    const TYPE_GOLD_REPORT_MASTER = 9;
    const TYPE_INDIVIDUAL_GOLD_SHEET = 11;
    const TYPE_SUMMARY = 3;
    const TYPE_SUMMARY_NEW = 302;
    const TYPE_SUMMARY_OFFSET = 4;
    const TYPE_SUMMARY_OFFSET_SCHEDULED = 45;
    const TYPE_FACTORED_INVOICES = 17;
    const TYPE_NON_FACTORED_INVOICES = 18;
    const TYPE_INVOICES = 20;
    const TYPE_DISCREPANCY = 21;
    const TYPE_SHIFT_NOSHOW = 23;
    const TYPE_SHIFT_CANCELLED_LESS_24 = 24;
    const TYPE_SHIFT_CANCELLED_NO_CALL_NO_SHOW = 41;
    const TYPE_SHIFT_LATE_ARRIVALS = 40;
    const TYPE_OPEN_SHIFTS = 26;
    const TYPE_CLIENT_AND_VENUE_DOCUMENTS = 31;
    const TYPE_EMPLOYEE_TENURE_AND_SHIFT_COUNT = 32;
    const TYPE_EMPLOYEE_DOCUMENTS = 29;
    const TYPE_PAYCHECK_MAILING_LIST = 7;
    const TYPE_1ST_SHIFT_EMPLOYEES = 25;
    const TYPE_DOUBLE_SHIFT = 12;
    const TYPE_CERTIFICATIONS = 33;
    const TYPE_EMPLOYEE_SHIFT_CANCELlATIONS = 34;
    const TYPE_EMPLOYEE_LIST = 101;
    const TYPE_BENEFIT_SICK = 42;
    const TYPE_TIMESHEET_EMPLOYEE_SUBMITTED_HOURS = 60;
    const TYPE_TIMESHEET_CLIENT_SUBMITTED_HOURS = 61;
    const TYPE_MSP_MARRIOTT = 80;
    const TYPE_OVER_40_HOURS = 14;
    const TYPE_OVER_40_HOURS_SCHEDULE_HOURS = 22;
    const TYPE_HUMAN_RESOURCES = 46;
    const TYPE_OVERTIMES = 100;
    const TYPE_NEW_HIRES = 102;
    const TYPE_CLIENT_BILLING = 103;

    public static function getBaseFields($params = array())
    {
        return [
            'type' => [
                'type' => 'dropdown',
                'data' => (function () {
                    $types = [
                        [
                            'id' => static::TYPE_CLIENTS,
                            'label' => 'Clients',
                            'queue' => true,
                            'fields' => [
                                'venue_id',
                                'tips_multiplier',
                                'msp_id',
                            ],
                        ],
                        [
                            'id' => static::TYPE_EMPLOYEE,
                            'label' => 'Employee',
                            'queue' => true,
                            'fields' => [
                                'employee_id',
                            ],
                        ],
                        [
                            'id' => static::TYPE_ALL_CLIENTS_EMPLOYEES,
                            'label' => 'All Clients and Employees',
                            'queue' => true,
                            'fields' => [
                                'tips_multiplier',
                            ],
                        ],
                        [
                            'id' => static::TYPE_ALL_CLIENTS_EMPLOYEES_NEW,
                            'label' => 'All Clients and Employees (new)',
                            'queue' => true,
                            'fields' => [
                                'venue_id',
                            ],
                        ],
                        [
                            'id' => static::TYPE_SUMMARY,
                            'label' => 'Summary Report',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_SUMMARY_NEW,
                            'label' => 'Summary Report (new)',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_SUMMARY_OFFSET,
                            'label' => 'Summary Report (offset)',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_SUMMARY_OFFSET_SCHEDULED,
                            'label' => 'Summary Report (offset-scheduled)',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_PAYCHECK_MAILING_LIST,
                            'label' => 'Paycheck Mailing List',
                            'queue' => false,
                        ],
                        [
                            'id' => static::TYPE_GOLD_REPORT_MASTER,
                            'label' => 'Gold Report Master',
                            'queue' => false,
                        ],
                        [
                            'id' => static::TYPE_INDIVIDUAL_GOLD_SHEET,
                            'label' => 'Individual Gold Sheet',
                            'queue' => false,
                        ],
                        [
                            'id' => static::TYPE_DOUBLE_SHIFT,
                            'label' => 'Double Shift',
                            'queue' => false,
                        ],
                        [
                            'id' => 13,
                            'label' => '7 Days Worked',
                            'queue' => false,
                        ],
                        [
                            'id' => static::TYPE_OVER_40_HOURS, // 14,
                            'label' => 'Over 40 Hours',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_OVER_40_HOURS_SCHEDULE_HOURS, // 22,
                            'label' => 'Over 40 Hours (schedule hours)',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_OVERTIMES,
                            'label' => 'Overtimes',
                            'queue' => true,
                        ],
                        [
                            'id' => 15,
                            'label' => 'Sick Pay Report',
                            'queue' => false,
                        ],
                        [
                            'id' => 16,
                            'label' => 'Wage Replacement Report',
                            'queue' => false,
                        ],
                        [
                            'id' => static::TYPE_FACTORED_INVOICES,
                            'label' => 'Factored Invoices',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_NON_FACTORED_INVOICES,
                            'label' => 'Non-Factored Invoices',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_INVOICES,
                            'label' => 'Invoices',
                            'queue' => true,
                            'fields' => [
                                'invoie_start_no',
                            ],
                        ],
                        [
                            'id' => static::TYPE_DISCREPANCY,
                            'label' => 'Discrepancy',
                            'queue' => false,
                        ],
                        [
                            'id' => static::TYPE_SHIFT_NOSHOW,
                            'label' => 'Shift No Show',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_SHIFT_CANCELLED_LESS_24,
                            'label' => 'Shift Cancelled Less 24',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_SHIFT_CANCELLED_NO_CALL_NO_SHOW,
                            'label' => 'Shift Cancelled No Call No Show',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_SHIFT_LATE_ARRIVALS,
                            'label' => 'Shift Late Arrivals',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_1ST_SHIFT_EMPLOYEES,
                            'label' => '1st Shift - Employees',
                            'queue' => false,
                        ],
                        [
                            'id' => static::TYPE_OPEN_SHIFTS,
                            'label' => 'Open Shifts',
                            'queue' => true,
                        ],
                        [
                            'id' => 27,
                            'label' => 'New Positions',
                            'queue' => true,
                        ],
                        [
                            'id' => 28,
                            'label' => 'Candidate Partners',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_CLIENT_AND_VENUE_DOCUMENTS,
                            'label' => 'Client and Venue Documents',
                            'queue' => true,
                            'fields' => [
                                'client_document_type_id',
                            ],
                        ],
                        [
                            'id' => static::TYPE_EMPLOYEE_TENURE_AND_SHIFT_COUNT,
                            'label' => 'Employee Tenure and Shift Count',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_EMPLOYEE_DOCUMENTS,
                            'label' => 'Employee Documents',
                            'queue' => true,
                            'fields' => [
                                'employee_document_type_id',
                            ],
                        ],
                        [
                            'id' => static::TYPE_CERTIFICATIONS,
                            'label' => 'Certifications',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_EMPLOYEE_SHIFT_CANCELlATIONS,
                            'label' => 'Employee Shift Cancellations',
                            'queue' => true,
                            'fields' => [
                                'employee_id',
                                'venue_id',
                                'employee_cancel_reason',
                            ],
                        ],
                        [
                            'id' => static::TYPE_TIMESHEET_VERIFICATION,
                            'label' => 'Timesheet Verification',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_TIMESHEET_VERIFICATION_CLIENTS,
                            'label' => 'Timesheet Verification Clients',
                            'queue' => true,
                            'fields' => [
                                'venue_id',
                            ],
                        ],
                        [
                            'id' => static::TYPE_EMPLOYEE_LIST,
                            'label' => 'Employee List',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_BENEFIT_SICK,
                            'label' => 'Benefit/Sick Pay',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_TIMESHEET_EMPLOYEE_SUBMITTED_HOURS,
                            'label' => 'Timesheet Employee Submitted Hours',
                            'queue' => true,
                            'fields' => [
                                'venue_id',
                                'employee_id',
                            ],
                        ],
                        [
                            'id' => static::TYPE_TIMESHEET_CLIENT_SUBMITTED_HOURS,
                            'label' => 'Timesheet Client Submitted Hours',
                            'queue' => true,
                            'fields' => [
                                'venue_id',
                                'employee_id',
                            ],
                        ],
                        [
                            'id' => static::TYPE_MSP_MARRIOTT,
                            'label' => 'MSP Marriott',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_NEW_HIRES,
                            'label' => 'New Hires',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_CLIENT_BILLING,
                            'label' => 'Client Billing',
                            'queue' => true,
                        ],
                        [
                            'id' => static::TYPE_HUMAN_RESOURCES,
                            'label' => 'Human Resources',
                            'queue' => true,
                        ],
                    ];

                    foreach ($types as &$type) {
                        if (!isset($type['fields'])) {
                            $type['fields'] = [];
                        }
                    }

                    return $types;
                })(),
                'id' => 'type',
            ],
            'employee_id' => [
                'type' => 'autocomplete',
                'url' => [
                    'path' => '/v1/employees/autocomplete',
                ],
                'id' => 'employee_id',
            ],
            'client_id' => [
                'type' => 'autocomplete',
                'url' => [
                    'path' => '/v1/clients/autocomplete',
                ],
                'id' => 'client_id',
            ],
            'venue_id' => [
                'type' => 'autocomplete',
                'url' => [
                    'path' => '/v1/venues/autocomplete',
                ],
                'id' => 'venue_id',
            ],
            'msp_id' => [
                'id' => 'msp_id',
                'type' => 'dropdown',
                'data' => ArrayHelper::map(Msp::find()->all(), 'id', 'name'),
            ],
            'date_range' => [
                'type' => 'daterange',
                'default' => call_user_func(function () {
                    return explode(' - ', ((string) Preference::get('payroll_filter_date_range')));
                }),
            ],
            'client_document_type_id' => [
                'id' => 'client_document_type_id',
                'type' => 'dropdown',
                'data' => DocumentType::getReportableLabels(DocumentType::TYPE_CLIENT_VENUE),
            ],
            'employee_document_type_id' => [
                'id' => 'employee_document_type_id',
                'type' => 'dropdown',
                'data' => DocumentType::getReportableLabels(DocumentType::TYPE_EMPLOYEE),
            ],
            'employee_cancel_reason' => [
                'id' => 'employee_cancel_reason',
                'type' => 'dropdown',
                'data' => ShiftEmployee::getEmployeeCancelReasonLabels(),
            ],
        ];
    }
}
